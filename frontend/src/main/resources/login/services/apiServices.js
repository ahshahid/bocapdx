app.factory('ApiFactory', ['$resource',
	function ($resource) {
        return {
            login: $resource('http://35.202.5.109:9090/api/login'),
            schema: $resource('http://35.202.5.109:9090/api/schema')
        }
		
	}
]);
/*
{
     *     get: {method: 'GET'},
     *     save: {method: 'POST'},
     *     query: {method: 'GET', isArray: true},
     *     remove: {method: 'DELETE'},
     *     delete: {method: 'DELETE'}
     *   } 
 */