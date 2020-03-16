app.controller('loginController', ['$scope', '$rootScope', '$http', 'ApiFactory', '$state', function($scope, $rootScope, $http, ApiFactory, $state) {

    $scope.username ="app"
    $scope.password = "app"
    $rootScope.isAuthenticated = false;
    /* Dummy data */
    /* $scope.ApiFactory = {
        login: {"results":["table1","table"]}
    } */
    $scope.login = function() {
    var data = {
      username: $scope.username,
      password: $scope.password
    };
   /*  $scope.tables = $scope.ApiFactory.login.results;
    $state.go('dashboard', {table : $scope.tables});
        $rootScope.isAuthenticated= true; */
     
    //call login api
   ApiFactory.login.save({
        username: data.username,
        password: data.password
    }, function (response) {
        $scope.tables = response.results;
        $state.go('dashboard', {table : $scope.tables});
        $rootScope.isAuthenticated= true;
    });
    
    }
    
	/* $http.post("http://35.202.5.109:9090/api/login",
	    JSON.stringify(data), { headers: { 'Access-Control-Allow-Headers':'origin, x-requested-with, content-type'} })
	    .then(function(response) {
        //response.data.schema.columns.sort(function (c1, c2) { return c1.name.localeCompare(c2.name); })
	    $scope.tables = response.data.results;
	    //$scope.pg_url = $scope.postgresstr;
      //  $scope.spice = $scope.postgresstr;
	    });
    } */

}])