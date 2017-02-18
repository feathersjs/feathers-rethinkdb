'use strict';

const rethink = require('rethinkdbdash');
const feathers = require('feathers');
const rest = require('feathers-rest');
const socketio = require('feathers-socketio');
const bodyParser = require('body-parser');
const service = require('../lib');
// `../lib` is for testing the example with the current build
// if you want to use the example use
// const service = require('feathers-rethinkdb');
const r = rethink({
  db: 'feathers'
});

let counter = 0;
const todoService = service({
  Model: r,
  name: 'todos',
  paginate: {
    default: 2,
    max: 4
  }
}).extend({
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
    data.counter = ++counter;
    return this._super(data, params);
  }
});

// Create a feathers instance.
let app = feathers()
  // Enable REST services
  .configure(rest())
  // Enable Socket.io services
  .configure(socketio())
  // Turn on JSON parser for REST services
  .use(bodyParser.json())
  // Turn on URL-encoded parser for REST services
  .use(bodyParser.urlencoded({
    extended: true
  }));

module.exports = todoService
  .init()
  .then(() => {
    // mount the service
    app.use('/todos', todoService);
    // start the server
    return app.listen(3030);
  });
