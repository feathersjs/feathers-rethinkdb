/**
 * Pass in a query object to get a ReQL query
 * Must be run after special query params are removed.
 */
export default function parseQuery (service, reQuery, params) {
  let r = service.options.r;

  Object.keys(params).forEach(qField => {
    let isFilter = false;
    let subQuery;
    // The queryObject's value: 'Alice'
    let qValue = params[qField];

    // If the qValue is an object, it will have special params in it.
    if (typeof qValue === 'object') {
      switch (Object.keys(qValue)[0]) {
        /**
         *  name: { $in: ['Alice', 'Bob'] }
         *  becomes
         *  r.expr(['Alice', 'Bob']).contains(doc['name'])
         */
        case '$in':
          isFilter = true;
          reQuery = reQuery.filter(function (doc) {
            return service.options.r.expr(qValue.$in).contains(doc(qField));
          });
          break;
        case '$nin':
          isFilter = true;
          reQuery = reQuery.filter(function (doc) {
            return service.options.r.expr(qValue.$nin).contains(doc(qField)).not();
          });
          break;
        case '$search':
          isFilter = true;
          reQuery = reQuery.filter(function (doc) {
            return doc(qField).match(qValue.$regexp);
          });
          break;
        case '$contains':
          isFilter = true;
          reQuery = reQuery.filter(function (doc) {
            return doc(qField).contains(qValue.$contains);
          });
          break;
        case '$lt':
          subQuery = r.row(qField).lt(params[qField].$lt);
          break;
        case '$lte':
          subQuery = r.row(qField).le(params[qField].$lte);
          break;
        case '$gt':
          subQuery = r.row(qField).gt(params[qField].$gt);
          break;
        case '$gte':
          subQuery = r.row(qField).ge(params[qField].$gte);
          break;
        case '$ne':
          subQuery = r.row(qField).ne(params[qField].$ne);
          break;
        case '$eq':
          subQuery = r.row(qField).eq(params[qField].$eq);
          break;
      }
    } else {
      subQuery = r.row(qField).eq(qValue);
    }

    // At the end of the current set of attributes, determine placement.
    if (subQuery) {
      reQuery = reQuery.filter(subQuery);
    } else if (!isFilter) {
      reQuery = reQuery.and(subQuery);
    }
  });
  return reQuery;
}
