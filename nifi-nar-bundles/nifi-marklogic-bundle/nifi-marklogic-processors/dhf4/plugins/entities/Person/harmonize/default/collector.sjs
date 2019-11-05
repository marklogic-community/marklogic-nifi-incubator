/*
 * Collect IDs plugin
 *
 * @param options - a map containing options. Options are sent from Java
 *
 * @return - an array of ids or uris
 */
function collect(options) {
   // by default we return the URIs in the same collection as the Entity name
  	xdmp.log(options);
  	var limit = options != null && options.hasOwnProperty('dhf.limit') ?
		parseInt(options['dhf.limit']) :
		Number.MAX_SAFE_INTEGER;

	return cts.uris(null, 'limit=' + limit, cts.trueQuery());
}

module.exports = {
  collect: collect
};
