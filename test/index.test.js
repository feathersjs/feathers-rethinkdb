import chai from 'chai';
import { base, example }
from 'feathers-service-tests';
import feathers from 'feathers';
import errors from 'feathers-errors';
import rethink from 'rethinkdbdash';
import service from '../src';

const r = rethink({
  db: 'feathers'
});

// RethinkDB: if no other sort order is given. This means that items can not be returned in the
// same order they have been created so this counter is used for sorting instead.
let counter = 0;

const expect = chai.expect;
const _ids = {};
const app = feathers().use('/people', service({
  Model: r,
  name: 'people',
  watch: true
}).extend({
  _find(params) {
    params = params || {};
    params.query = params.query || {};
    if (!params.query.$sort) {
      params.query.$sort = {
        counter: 1
      };
    }

    return this._super(params);
  },

  create(data, params) {
    return this._super(Object.assign({}, data, {counter: ++counter }), params);
  }
}));
const people = app.service('people');

function clean(done) {
  r.table('people').delete(null).run()
    .then(() => r.table('todos').delete().run())
    .then(() => done())
    .catch(done);
}

function create(done) {
  counter = 0;
  // Create the db if it doesn't exist.
  r.dbList().contains('feathers').do(databaseExists => r.branch(
      databaseExists, {
        created: 0
      },
      r.dbCreate('feathers')))
    .run()
    // Create the todos table if it doesn't exist.
    .then(() => {
      const table = r.db('feathers');

      return Promise.all([
        table.tableList().contains('todos')
          .do(function(tableExists) {
            return r.branch(
              tableExists, {
                created: 0
              },
              table.tableCreate('todos')
            );
          }).run(),
        table.tableList().contains('people')
          .do(function(tableExists) {
            return r.branch(
              tableExists, {
                created: 0
              },
              table.tableCreate('people')
            );
          }).run()
      ]);
    })
    .then(() => {
      app.setup();
      done();
    })
    .catch(done);
}

describe('feathers-rethinkdb', () => {

  before(create);
  after(clean);

  beforeEach(done => {
    people.create({
        name: 'Doug',
        age: 32
      }, {})
      .then(data => {
        _ids.Doug = data.id;
        done();
      })
      .catch(err => {
        done(err);
      });
  });

  afterEach(() => people.remove(null));

  it('is CommonJS compatible', () => {
    expect(typeof require('../lib')).to.equal('function');
  });

  it('basic functionality', done => {
    expect(typeof 1).to.equal('number');
    done();
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

  base(people, _ids, errors.types);
});

describe('RethinkDB service example test', () => {
  let server;

  before(() => server = require('../example/app'));
  after(done => server.close(() => done()));

  example('id');
});
