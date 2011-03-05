function(v, keydata, args) {
	if (v.values) {
		var ret = [];
		o = Riak.mapValuesJson(v)[0];
		o.lastModifiedParsed = Date.parse(v['values'][0]['metadata']['X-Riak-Last-Modified']);
		o.key = v['key'];
		ret.push(o);
		return ret;
	} else {
		return [];
	}
}