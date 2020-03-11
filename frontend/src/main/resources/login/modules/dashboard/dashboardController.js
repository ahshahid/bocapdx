app.controller('dashboardController', ['$scope', '$http', 'ApiFactory', '$stateParams', 'NgTableParams', function($scope, $http, ApiFactory, $stateParams, NgTableParams) {

    /* $scope.ApiFactory = {
        schema: {"columns":[{"name":"aa"},{"type":"ass"}]}
    } */
   
    $scope.myTable = {
        selected:{}
    };
    $scope.temp= {}
    $scope.schemaCols = {};
    $scope.selectedCols = [];
    $scope.workSheetTables = [];
    $scope.columnList = {};
    $scope.schemaRows = {};
    $scope.rowCount = 0;
    $scope.tableNames  = $stateParams.table;


    $scope.dragOptions = {
        /* start: function(e) {
            console.log("Start");
        },
        drag: function(e) {
          console.log("DRAGGING");
        },
        stop: function(e) {
          console.log("STOPPING");
        }, */
        container: 'worksheet'
    }
    
    $scope.getData = function(table) {
           $scope.tableName = table;
           $scope.clearColumnSelection();
           console.log($scope.myTable);
           $scope.worksheet=true;
           $scope.temp =[];
            ApiFactory.schema.save({
                tablename: table
            }, function (response) {
                $scope.columnList = response.schema.columns;
                var newJson=[]
                angular.forEach(response.schema.columns, function(value,key) {
                    var name =value.name
                    
                    var abc = {
                        "name": name,
                        "type": value.type,
                        "title": name,
                        "sortable": name,
                        "filter": {
                            name: "text"
                        },
                        "show": true,
                        "field":name
                    }
                    newJson.push(abc);
    
                })
                $scope.schemaCols = newJson;
            });
            ApiFactory.getRows.save({
                tablename: table
            }, function (response) {
                var data = response.rows;
                $scope.rowCount = response.rowCount;
               $scope.convertKeyValueJson($scope.schemaCols, response.rows);
               $scope.schemaRows =  new NgTableParams({page: 1,
                count: 10,
                filter: {},
                sorting: {}}, 
                {dataset: $scope.temp});
            });
            var tempObj = {
                rowCount: $scope.rowCount,
                colCount: $scope.columnList.length,
                name: table
            }
            $scope.workSheetTables.push(tempObj);
    }

    $scope.convertKeyValueJson = function(column, rows){
            var headers=[];
            angular.forEach(column, function(value,key) {
                headers.push(value.name)

            })
            //$scope.schemaCols= ["field1", "field2", "field3"];
            angular.forEach(rows, function(value) {
                var result =  value.reduce(function(result, field, index) {
                    result[headers[index]] = field;
                    return result;
                  }, {})
                  $scope.temp.push(result);
            });
    }

    $scope.toggleColumnSelection = function(columnName){
            if($scope.selectedCols.indexOf(columnName) == -1) {
                $scope.selectedCols.push(columnName);
                var sel = document.querySelectorAll('col#'+columnName)[0].className='active';
              }else{
                var index = $scope.selectedCols.indexOf(columnName);
                $scope.selectedCols.splice(index, 1);
                var sel = document.querySelectorAll('col#'+columnName)[0].classList.remove('active');
              }
        console.log($scope.selectedCols);
            
           // console.log(document.querySelectorAll('col#'+columnName));
    }
        
    $scope.clearColumnSelection = function(){
        $scope.selectedCols =[];
    }

    $scope.goToTab = function(tabName){

    }

}])