app.factory('ApiFactory', ['$resource',
	function ($resource) {
        const server = 'http://35.224.182.196:9090/';
        return {
            login: $resource(server +'api/login'),
            schema: $resource(server +'api/schema')
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