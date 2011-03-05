function ( v , args ) {
	var field = args[0];                                                                                                                                                                             
	var reverse = args[1];
	v.sort(function(a, b) {
		if (reverse) {
			var _ref = [b, a];
			a = _ref[0];
			b = _ref[1];
		}
		if (((typeof a === 'undefined' || a === null) ? undefined :
			a[field]) < ((typeof b === 'undefined' || b === null) ? undefined :
				b[field])) {
			return -1;
		} else if (((typeof a === 'undefined' || a === null) ? undefined :
			a[field]) === ((typeof b === 'undefined' || b === null) ? undefined :
				b[field])) {
			return 0;
		} else if (((typeof a === 'undefined' || a === null) ? undefined :
			a[field]) > ((typeof b === 'undefined' || b === null) ? undefined :
				b[field])) {
			return 1;
		}
	});                       
	return v
}
