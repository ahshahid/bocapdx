app.controller('dashboardController', ['$scope', '$http', 'ApiFactory', '$stateParams', function($scope, $http, ApiFactory, $stateParams) {

        $scope.tableName = $stateParams.table;
        $scope.schemaCols = {};

        $scope.getData = function() {
            ApiFactory.schema.save({
                tablename: $scope.tableName
            }, function (response) {
                $scope.schemaCols = response.schema.columns;
            });
            }

}])