const chai = require('chai');
const { base } = require('feathers-service-tests');

const feathers = require('@feathersjs/feathers');
const errors = require('@feathersjs/errors');
const rethink = require('rethinkdbdash');
const service = require('../lib');

const r = rethink({
  db: 'feathers'
});

// RethinkDB: if no other sort order is given. This means that items can not be returned in the
// same order they have been created so this counter is used for sorting instead.
let counter = 0;

const expect = chai.expect;
const numberService = {
  _find (params) {
    params = params || {};
    params.query = params.query || {};
    if (!params.query.$sort) {
      params.query.$sort = {
        counter: 1
      };
    }

    return this._super(params);
  },

  create (data, params) {
    const addCount = current => Object.assign({}, current, {
      counter: ++counter
    });

    if (Array.isArray(data)) {
      data = data.map(addCount);
    } else {
      data = addCount(data);
    }

    return this._super(data, params);
  }
};

const app = feathers()
  .use('/people', service({
    Model: r,
    name: 'people',
    watch: true,
    events: ['testing']
  }).extend(numberService))
  .use('/people-customid', service({
    id: 'customid',
    Model: r,
    name: 'people_customid',
    watch: true,
    events: ['testing']
  }).extend(numberService));
const people = app.service('people');

people.hooks({
  after (hook) {
    hook.result.test = 'testing';
  }
});

describe('feathers-rethinkdb', () => {
  before(() => {
    return r.dbList().contains('feathers') // create db if not exists
      .do(dbExists => r.branch(
        dbExists,
        { created: 0 },
        r.dbCreate('feathers')
      ))
      .run().then(() => Promise.all([
        app.service('people').init(),
        app.service('people-customid').init({
          primaryKey: 'customid'
        })
      ])).then(() => app.setup());
  });

  after(() => {
    return Promise.all([
      r.table('people').delete(null),
      r.table('people_customid').delete(null)
    ]);
  });

  it('is CommonJS compatible', () => {
    expect(typeof require('../lib')).to.equal('function');
  });

  it('basic functionality', () => {
    expect(typeof 1).to.equal('number');
  });

  it('after hooks run and get send with events', done => {
    const name = 'Hooks tester';
    const service = app.service('people');

    service.once('created', person => {
      try {
        expect(person.test).to.equal('testing', 'Hook property got set');
      } catch (e) {
        done(e);
      }
    });

    service.once('removed', person => {
      try {
        expect(person.test).to.equal('testing', 'Hook property got set');
        done();
      } catch (e) {
        done(e);
      }
    });

    service.create({
      name,
      age: 1
    }).then(person => service.remove(person.id));
  });

  describe('common tests', () => {
    base(app, errors);
    base(app, errors, 'people-customid', 'customid');
  });

  describe('Changefeeds', () => {
    it('`created` and `removed`', done => {
      const table = r.db('feathers').table('people');

      people.once('created', person => {
        expect(person.name).to.equal('Marshall Thompson');
        expect(person.counter).to.equal(counter);
        table.get(person.id).delete().run();
      });

      people.once('removed', person => {
        expect(person.name).to.equal('Marshall Thompson');
        done();
      });

      table.insert({
        name: 'Marshall Thompson',
        counter: ++counter
      }).run();
    });

    it('`patched` and `updated`', done => {
      const table = r.db('feathers').table('people');

      people.once('created', person => {
        expect(person.name).to.equal('Marshall Thompson');
        person.name = 'Marshall T.';
        table.get(person.id).replace(person).run();
      });

      people.once('patched', person => expect(person.name).to.equal('Marshall T.'));

      people.once('updated', person => {
        expect(person.name).to.equal('Marshall T.');
        table.get(person.id).delete().run();
      });

      people.once('removed', () => done());

      table.insert({
        name: 'Marshall Thompson',
        counter: ++counter
      }).run();
    });
  });

  describe('creates', () => {
    it('create works with an array', () => {
      return people.create([{ name: 'Test 1' }, { name: 'Test 2' }])
        .then(data => {
          expect(typeof data[0].id).to.not.equal('undefined');
          expect(typeof data[1].id).to.not.equal('undefined');
        });
    });

    it('create allows upsert with params.rethinkdb options', () => {
      people.create({ name: 'Testing upser' }).then(result => {
        const rethinkdb = { conflict: 'update' };

        result.name = 'Testing upsert';

        return people.create(result, { rethinkdb });
      }).then(result => expect(result.name).to.equal('Testing upsert'));
    });
  });

  describe('additional query parameters', () => {
    it('$search', () => {
      return people.create([{
        name: 'Dave'
      }, {
        name: 'Ddave'
      }, {
        name: 'Eric'
      }]).then(() => people.find({
        query: { name: { $search: '^Da' } }
      })).then(page => {
        expect(page.length, 1);
        expect(page[0].name).to.equal('Dave');
      }).then(() => people.find({
        query: { name: { $search: 've$' } }
      })).then(page => {
        expect(page.length, 2);
        expect(page[0].name).to.equal('Dave');
        expect(page[1].name).to.equal('Ddave');
      });
    });

    it('$contains', () => {
      return people.create([{
        name: 'David',
        nickNames: ['Dave', 'David', 'Feathers guy']
      }, {
        name: 'Eric',
        nickNames: ['Eric', 'E', 'Feathers guy']
      }]).then(() => people.find({
        query: { nickNames: { $contains: 'Dave' } }
      })).then(page => {
        expect(page.length, 1);
        expect(page[0].name).to.equal('David');
      }).then(() => people.find({
        query: { nickNames: { $contains: 'Feathers guy' } }
      })).then(page => {
        expect(page.length, 2);
        expect(page[0].name).to.equal('David');
        expect(page[1].name).to.equal('Eric');
      });
    });

    it('$and', () => {
      return people.create([{
        name: 'Dave',
        age: 23,
        hobby: 'fishing'
      }, {
        name: 'John',
        age: 10,
        hobby: 'archery'
      }, {
        name: 'Eva',
        age: 30,
        hobby: 'archery'
      }]).then(() => people.find({
        query: {
          $and:
          [{
            age: { $gt: 18 }
          },
          {
            $or:
            [{ hobby: { $eq: 'archery' } },
            { hobby: { $eq: 'fishing' } }]
          }]
        }
      })).then(page => {
        expect(page.length, 2);
        expect(page[0].name).to.equal('Dave');
        expect(page[1].name).to.equal('Eva');
      })
        .then(() => people.find({
          query: {
            $and:
            [{
              age: { $gt: 18 }
            },
            { hobby: { $eq: 'fishing' } },
            { name: { $eq: 'Dave' } }]
          }
        })).then(page => {
          expect(page.length, 1);
          expect(page[0].name).to.equal('Dave');
        });
    });
  });
});

describe('init database', () => {
  it('service.init() initializes the database', () => {
    return service({ Model: r, name: 'testTable' })
      .init()
      .then(() => {
        expect(r.tableList().contains('testTable'));
        r.table('testTable').delete(null).run();
      });
  });
});

describe('find within nested document', () => {
  it('$search', () => {
    return people.create([{
      name: 'Dave',
      postalAddress: {
        city: 'Melbourne',
        country: 'US',
        state: 'FL',
        street: '123 6th St',
        zipcode: '32904'
      }
    }, {
      name: 'Tom',
      postalAddress: {
        city: 'Chevy Chase',
        country: 'US',
        state: 'MD',
        street: '71 Pilgrim Avenue',
        zipcode: '20815'
      }
    }, {
      name: 'Eric',
      postalAddress: {
        city: 'Honolulu',
        country: 'US',
        state: 'HI',
        street: '4 Goldfield St',
        zipcode: '96815'
      }
    }]).then(() => people.find({
      query: { 'postalAddress.state': { $search: '^F' } }
    })).then(page => {
      expect(page.length, 1);
      expect(page[0].postalAddress.state).to.equal('FL');
    }).then(() => people.find({
      query: { 'postalAddress.street': { $search: 'St$' } }
    })).then(page => {
      expect(page.length, 2);
      expect(page[0].name).to.equal('Dave');
      expect(page[1].name).to.equal('Eric');
    });
  });
});
