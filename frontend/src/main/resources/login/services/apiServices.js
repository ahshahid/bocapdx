app.factory('ApiFactory', ['$resource',
	function ($resource) {

        /* for server comment below const */
         const server = 'http://34.69.42.63:9090/';

        /* For local comment below const */
        //const server = ''; 
        return {
            login: $resource(server +'api/login'),
            schema: $resource(server +'api/schema'),
            getRows: $resource(server +'api/sampleRows')
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