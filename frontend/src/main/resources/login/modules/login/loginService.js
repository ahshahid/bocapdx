app.factory('ApiFactory', ['$resource',
	function ($resource) {
        return {
            login: $resource('http://35.202.5.109:9090/api/login'),
            schema: $resource('http://35.202.5.109:9090/api/login')
        }
		
	}
]);