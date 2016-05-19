import Proto from 'uberproto';
import filter from 'feathers-query-filters';
import { types as errors }
from 'feathers-errors';
import parseQuery from './parse';

// Create the service.
class Service {
  constructor(options) {
    if (!options) {
      throw new Error('RethinkDB options have to be provided.');
    }

    if (options.Model) {
      options.r = options.Model;
    } else {
      throw new Error('You must provide the RethinkDB object on options.Model');
    }

    // Make sure the user connected a database before creating the service.
    if (!options.r._poolMaster._options.db) {
      throw new Error('You must provide either an instance of r that is preconfigured with a db, or a provide options.db.');
    }

    if (!options.name) {
      throw new Error('You must provide a table name on options.name');
    }

    this.type = 'rethinkdb';
    this.id = options.id || 'id';
    this.table = options.r.table(options.name);
    this.options = options;
    this.paginate = options.paginate || {};
    this.events = ['created', 'updated', 'patched', 'removed'];
  }

  extend(obj) {
    return Proto.extend(obj, this);
  }

  _find(params = {}) {
    let r = this.options.r;

    params.query = params.query || {};

    // Start with finding all, and limit when necessary.
    let query = this.table.filter({});
    // Prepare the special query params.
    let filters = filter(params.query, this.paginate);

    // Handle $select
    if (filters.$select) {
      query = query.pluck(filters.$select);
    }

    // Handle $sort
    if (filters.$sort) {
      let fieldName = Object.keys(filters.$sort)[0];
      if (filters.$sort[fieldName] === 1) {
        query = query.orderBy(fieldName);
      } else {
        query = query.orderBy(r.desc(fieldName));
      }
    }

    // Handle $or
    // TODO (@marshallswain): Handle $or queries with nested specials.
    // Right now they won't work and we'd need to start diving
    // into nested where conditions.
    if (params.query.$or) {
      // orQuery will be built and passed to row('rowName').filter().
      let orQuery;
      // params.query.$or looks like [ { name: 'Alice' }, { name: 'Bob' } ]
      // Needs to become:
      // r.row("name").eq('Alice').or(r.row("name").eq('Bob'))
      params.query.$or.forEach((queryObject, i) => {
        // queryObject looks like { name: 'Alice' }
        let keys = Object.keys(queryObject);

        keys.forEach(qField => {
          // The queryObject's value: 'Alice'
          let qValue = queryObject[qField];

          // Build the subQuery based on the qField.
          let subQuery;
          // If the qValue is an object, it will have special params in it.
          if (typeof qValue !== 'object') {
            subQuery = r.row(qField).eq(qValue);
          }

          // At the end of the current set of attributes, determine placement.
          if (i === 0) {
            orQuery = subQuery;
          } else {
            orQuery = orQuery.or(subQuery);
          }
        });
      });

      query = query.filter(orQuery);
      delete params.query.$or;
    }
    query = parseQuery(this, query, params.query);

    let countQuery;

    // For pagination, count has to run as a separate query, but without limit.
    if (this.paginate.default) {
      countQuery = query.count().run();
    }

    // Handle $skip AFTER the count query but BEFORE $limit.
    if (filters.$skip) {
      query = query.skip(filters.$skip);
    }
    // Handle $limit AFTER the count query and $skip.
    if (filters.$limit) {
      query = query.limit(filters.$limit);
    }

    // Execute the query
    return Promise.all([query, countQuery]).then(([data, total]) => {
      if (this.paginate.default) {
        return {
          total,
          data,
          limit: filters.$limit,
          skip: filters.$skip || 0
        };
      }

      return data;
    });
  }

  find(...args) {
    return this._find(...args);
  }

  _get(id, params) {
    let query;
    // If an id was passed, just get the record.
    if (id !== null && id !== undefined) {
      query = this.table.get(id);

      // If no id was passed, use params.query
    } else {
      params = params || {
        query: {}
      };
      query = this.table.filter(params.query).limit(1);
    }

    return query.run().then(data => {
      if (Array.isArray(data)) {
        data = data[0];
      }
      if (!data) {
        throw new errors.NotFound(`No record found for id '${id}'`);
      }
      return data;
    });
  }

  get(...args) {
    return this._get(...args);
  }

  // STILL NEED TO ADD params argument here.
  create(data) {
    return this.table.insert(data).run()
      .then(res => Object.assign({
        id: (data.id ? data.id : res.generated_keys[0])
      }, data));
  }

  patch(id, data, params) {
    let query;

    if (id !== null && id !== undefined) {
      query = this._get(id);
    } else if (params) {
      query = this._find(params);
    } else {
      return Promise.reject(new Error('Patch requires an ID or params'));
    }

    // Find the original record(s), first, then patch them.
    return query.then(getData => {
      let query;
      if (Array.isArray(getData)) {
        query = this.table.getAll(...getData.map(item => item.id));
      } else {
        query = this.table.get(id);
      }
      return query.update(data, {
        returnChanges: true
      }).run().then(response => {
        let changes = response.changes.map(change => change.new_val);
        return changes.length === 1 ? changes[0] : changes;
      });
    });
  }

  update(id, data) {
    return this._get(id).then(getData => {
      data.id = id;
      return this.table.get(getData.id)
        .replace(data, {
          returnChanges: true
        }).run()
        .then(result =>
          (result.changes && result.changes.length) ? result.changes[0].new_val : data
        );
    });
  }

  remove(id, params) {
    let query;

    // You have to pass id=null to remove all records.
    if (id !== null && id !== undefined) {
      query = this.table.get(id);
    } else if (id === null) {
      const queryParams = Object.assign({}, params && params.query);
      query = this.table.filter(queryParams);
    } else {
      return Promise.reject(new Error('You must pass either an id or params to remove.'));
    }

    return query.delete({
      returnChanges: true
    }).run().then(res => {
      if (res.changes && res.changes.length) {
        let changes = res.changes.map(change => change.old_val);
        return changes.length === 1 ? changes[0] : changes;
      } else {
        return [];
      }
    });
  }

  setup() {
    this._cursor = this.table.changes().run().then(cursor => {
      cursor.each((error, data) => {
        if (error || typeof this.emit !== 'function') {
          return;
        }
        if (data.old_val === null) {
          this.emit('created', data.new_val);
        } else if (data.new_val === null) {
          this.emit('removed', data.old_val);
        } else {
          this.emit('updated', data.new_val);
          this.emit('patched', data.new_val);
        }
      });

      return cursor;
    });
  }
}

export default function init(options) {
  return new Service(options);
}

init.Service = Service;
