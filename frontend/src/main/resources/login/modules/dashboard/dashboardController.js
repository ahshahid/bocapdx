app.controller('dashboardController', ['$scope', '$http', 'ApiFactory', '$stateParams', 'NgTableParams', function($scope, $http, ApiFactory, $stateParams, NgTableParams) {

    $scope.tableName = $stateParams.table;
        $scope.schemaCols = {};
        $scope.schemaRows = {};
        $scope.getData = function() {
            ApiFactory.schema.save({
                tablename: $stateParams.table
            }, function (response) {
                $scope.schemaCols = response.schema.columns;
               /*  $scope.schemaCols =  new NgTableParams({page: 1,
                    count: 1,
                    filter: {},
                    sorting: {}}, {dataset: data}); */
            });
            ApiFactory.getRows.save({
                tablename: $stateParams.table
            }, function (response) {
                var data = response.rows;
                console.log(response.rows);
                $scope.schemaRows =  new NgTableParams({page: 1,
                    count: 2,
                    filter: {},
                    sorting: {}}, {dataset: data});
            
            });
        }

}])