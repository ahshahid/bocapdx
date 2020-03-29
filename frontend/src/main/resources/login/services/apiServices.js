app.factory('ApiFactory', ['$resource',
	function ($resource) {

        /*for server */
        //const server = '';

        /*local server  */
        const server = 'http://localhost:3000/';

        return {
            login: $resource(server +'api/login'),
            refreshTables: $resource(server +'api/refreshTables'),
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