const { _, hooks } = require('@feathersjs/commons');
const { AdapterService, select } = require('@feathersjs/adapter-commons');
const errors = require('@feathersjs/errors');

const { createFilter, filterObject } = require('./parse');

const { processHooks, getHooks, createHookObject } = hooks;

// Create the service.
class Service extends AdapterService {
  constructor (options) {
    if (options.Model) {
      options.r = options.Model;
    } else {
      throw new Error('You must provide the RethinkDB object on options.Model');
    }

    if (!options.db) {
      if (options.r.pool && options.r.pool.connParam.db) {
        options.db = options.r.pool.connParam.db;
      }
      else if (!options.r.pool && options.r.singleconnection.db) {
        options.db = options.r.singleconnection.db;
      } else {
        throw new Error('You must provide db on options.db or use connectPool');
      }
    }

    if (!options.name) {
      throw new Error('You have to provide a table name on options.name');
    }

    if (!options.id) {
      options.id = 'id';
    }

    if (!options.whitelist) {
      options.whitelist = [
        '$contains',
        '$search',
        '$and',
        '$eq'
      ];
    }

    super(options);

    this.type = 'rethinkdb';
    this.table = options.r.db(options.db).table(options.name);
    this.options = options;
    this.watch = options.watch !== undefined ? options.watch : true;
    this.paginate = options.paginate || {};
  }

  get Model () {
    return this.options.Model;
  }

  _getOrFind (id, params) {
    if (id === null) {
      return this._find(params);
    }

    return this._get(id, params);
  }

  init (opts = {}) {
    const r = this.options.r;
    const t = this.options.name;
    const db = this.options.db;

    return r.dbList().contains(db) // create db if not exists
      .do(dbExists => r.branch(dbExists, { created: 0 }, r.dbCreate(db)))
      .run().then(() => {
        return r.db(db).tableList().contains(t) // create table if not exists
          .do(tableExists => r.branch(
            tableExists, { created: 0 },
            r.db(db).tableCreate(t, opts))
          ).run();
      });
  }

  createFilter (query) {
    return createFilter(query, this.options.r);
  }

  createQuery (originalQuery) {
    const { filters, query } = this.filterQuery(originalQuery || {});

    const r = this.options.r;
    let rq = this.table.filter(this.createFilter(query));

    // Handle $select
    if (filters.$select) {
      rq = rq.pluck(filters.$select);
    }

    // Handle $sort
    if (filters.$sort) {
      _.each(filters.$sort, (order, fieldName) => {
        if (parseInt(order) === 1) {
          rq = rq.orderBy(fieldName);
        } else {
          rq = rq.orderBy(r.desc(fieldName));
        }
      });
    }

    return rq;
  }

  _find (params = {}) {
    const paginate = typeof params.paginate !== 'undefined' ? params.paginate : this.paginate;
    // Prepare the special query params.
    const { filters } = this.filterQuery(params || {}, paginate);

    let q = params.rethinkdb || this.createQuery(params);
    let countQuery;

    // For pagination, count has to run as a separate query, but without limit.
    if (paginate.default) {
      countQuery = q.count().run();
    }

    // Handle $skip AFTER the count query but BEFORE $limit.
    if (filters.$skip) {
      q = q.skip(filters.$skip);
    }

    let limit;
    // Handle $limit AFTER the count query and $skip.
    if (typeof filters.$limit !== 'undefined') {
      limit = filters.$limit;
      q = q.limit(limit);
    } else if (paginate && paginate.default) {
      limit = paginate.default;
      q = q.limit(limit);
    }

    if (limit && paginate && paginate.max && limit > paginate.max) {
      limit = paginate.max;
    }

    q = q.run();

    // Execute the query
    return Promise.all([q, countQuery]).then(([data, total]) => {
      if (paginate && paginate.default) {
        return {
          total,
          data,
          limit: limit,
          skip: filters.$skip || 0
        };
      }

      return data;
    });
  }

  find (...args) {
    return this._find(...args);
  }

  _get (id, params = {}) {
    let query = this.table.filter(params.query).limit(1);

    // If an id was passed, just get the record.
    if (id !== null && id !== undefined) {
      query = this.table.get(id);
    }

    if (params.query && params.query.$select) {
      query = query.pluck(params.query.$select.concat(this.id));
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

  get (...args) {
    return this._get(...args);
  }

  create (data, params = {}) {
    const idField = this.id;

    let options = Object.assign({}, params);
    options = filterObject(options, ['returnChanges', 'durability', 'conflict']);

    return this.table.insert(data, options).run().then(res => {
      if (data[idField]) {
        if (res.errors) {
          return Promise.reject(new errors.Conflict('Duplicate primary key', res.errors));
        }
        return data;
      } else { // add generated id
        const addId = (current, index) => {
          if (res.generated_keys && res.generated_keys[index]) {
            return Object.assign({}, current, {
              [idField]: res.generated_keys[index]
            });
          }

          return current;
        };

        if (Array.isArray(data)) {
          return data.map(addId);
        }

        return addId(data, 0);
      }
    }).then(select(params, this.id));
  }

  patch (id, data, params = {}) {
    let query;
    let options = Object.assign({ returnChanges: true }, params);
    options = filterObject(options, ['returnChanges', 'durability', 'nonAtomic']);

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
        query = this.table.getAll(...getData.map(item => item[this.id]));
      } else {
        query = this.table.get(id);
      }

      return query.update(data, options).run().then(response => {
        const changes = response.changes.map(change => change.new_val);
        return changes.length === 1 ? changes[0] : changes;
      });
    }).then(select(params, this.id));
  }

  update (id, data, params = {}) {
    let options = Object.assign({ returnChanges: true }, params);
    options = filterObject(options, ['returnChanges', 'durability', 'nonAtomic']);

    if (Array.isArray(data) || id === null) {
      return Promise.reject(new errors.BadRequest('Not replacing multiple records. Did you mean `patch`?'));
    }

    return this._get(id, params).then(getData => {
      data[this.id] = id;
      return this.table.get(getData[this.id])
        .replace(data, options).run().then(result =>
          (result.changes && result.changes.length) ? result.changes[0].new_val : data
        );
    }).then(select(params, this.id));
  }

  remove (id, params = {}) {
    let query;
    let options = Object.assign({ returnChanges: true }, params);
    options = filterObject(options, ['returnChanges', 'durability', 'nonAtomic']);

    // You have to pass id=null to remove all records.
    if (id !== null && id !== undefined) {
      query = this.table.get(id);
    } else if (id === null) {
      query = this.createQuery(params);
    } else {
      return Promise.reject(new Error('You must pass either an id or params to remove.'));
    }

    return query.delete(options)
      .run()
      .then(res => {
        if (res.changes && res.changes.length) {
          const changes = res.changes.map(change => change.old_val);
          return changes.length === 1 ? changes[0] : changes;
        } else {
          return [];
        }
      }).then(select(params, this.id));
  }

  watchChangefeeds (app) {
    if (!this.watch || this._cursor) {
      return this._cursor;
    }

    let runHooks = (method, data) => Promise.resolve({
      result: data
    });

    if (this.__hooks) { // If the hooks plugin is enabled
      // This is necessary because the data coming directly from the
      // change feeds does not run through `after` hooks by default
      // so we have to do it manually
      runHooks = (method, data) => {
        const service = this;
        const args = [{ query: {} }];
        const hookData = {
          app,
          service,
          result: data,
          type: 'after',
          get path () {
            return Object.keys(app.services)
              .find(path => app.services[path] === service);
          }
        };

        // Add `data` to arguments
        if (method === 'create' || method === 'update' || method === 'patch') {
          args.unshift(data);
        }

        // `id` for update, patch and remove
        if (method === 'update' || method === 'patch' || method === 'remove') {
          args.unshift(data[this.id]);
        }

        const hookObject = createHookObject(method, args, hookData);
        hookObject.type = 'after'; //somehow this gets lost e. g. for protect('password') in createHookObject but is needed in processHooks isHookObject
        const hookChain = getHooks(app, this, 'after', method);

        return processHooks.call(this, hookChain, hookObject);
      };
    }

    this._cursor = this.table.changes().run().then(cursor => {
      cursor.each((error, data) => {
        if (error || typeof this.emit !== 'function') {
          return;
        }
        // For each case, run through processHooks first,
        // then emit the event
        if (data.old_val === null) {
          runHooks('create', data.new_val)
            .then(hook => {
              this.emit('created', hook);
            });
        } else if (data.new_val === null) {
          runHooks('remove', data.old_val)
            .then(hook => this.emit('removed', hook));
        } else {
          runHooks('patch', data.new_val).then(hook => {
            this.emit('updated', hook);
            this.emit('patched', hook);
          });
        }
      });

      return cursor;
    });

    return this._cursor;
  }

  setup (app) {
    const rethinkInit = app.get('rethinkInit') || Promise.resolve();
    const r = this.options.r;

    rethinkInit
      .then(() => r.pool.waitForHealthy())
      .then(() => this.watchChangefeeds(app));
  }
}

module.exports = function (options) {
  return new Service(options);
};

module.exports.Service = Service;
