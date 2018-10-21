const { _ } = require('@feathersjs/commons');

// Special parameter to RQL condition
const mappings = {
  $search: 'match',
  $contains: 'contains',
  $lt: 'lt',
  $lte: 'le',
  $gt: 'gt',
  $gte: 'ge',
  $ne: 'ne',
  $eq: 'eq'
};

exports.createFilter = function createFilter (query, r) {
  return function (doc) {
    const or = query.$or;
    const and = query.$and;
    let matcher = r({});

    // Handle $or. If it exists, use the first $or entry as the base matcher
    if (Array.isArray(or)) {
      matcher = createFilter(or[0], r)(doc);

      for (let i = 0; i < or.length; i++) {
        matcher = matcher.or(createFilter(or[i], r)(doc));
      }
    // Handle $and
    } else if (Array.isArray(and)) {
      matcher = createFilter(and[0], r)(doc);

      for (let i = 0; i < and.length; i++) {
        matcher = matcher.and(createFilter(and[i], r)(doc));
      }
    }

    _.each(query, (value, field) => {
      if (typeof value !== 'object') {
        // Match value directly
        matcher = matcher.and(buildNestedQueryPredicate(field, doc).eq(value));
      } else {
        // Handle special parameters
        _.each(value, (selector, type) => {
          if (type === '$in') {
            matcher = matcher.and(r.expr(selector).contains(buildNestedQueryPredicate(field, doc)));
          } else if (type === '$nin') {
            matcher = matcher.and(
              r.expr(selector).contains(buildNestedQueryPredicate(field, doc)).not()
            );
          } else if (mappings[type]) {
            const selectorArray = Array.isArray(selector) ? selector : [ selector ];
            const method = mappings[type];

            matcher = matcher.and(buildNestedQueryPredicate(field, doc)[method](...selectorArray));
          }
        });
      }
    });

    return matcher;
  };
};

function buildNestedQueryPredicate (field, doc) {
  var fields = field.split('.');
  var searchFunction = doc(fields[0]);

  for (var i = 1; i < fields.length; i++) {
    searchFunction = searchFunction(fields[i]);
  }

  return searchFunction;
}
