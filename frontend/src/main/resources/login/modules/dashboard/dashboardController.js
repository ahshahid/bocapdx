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
    $scope.isColumnRes = false;
    $scope.isRowRes = false;

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
    $scope.refreshTables = function(){
        ApiFactory.refreshTables.get({
        }, function (response) {
             $scope.tables = response.results;
        })
    }
    $scope.getData = function(table,dragged) {
       
           $scope.tableName = table;
           $scope.clearColumnSelection();
           console.log($scope.myTable.selected);
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
                    $scope.isColumnRes = true;
                })
                $scope.schemaCols = newJson;
                $scope.createWorkSheetTables(table);
            });

           //$scope.createJsonForSchema(table);

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
                $scope.isRowRes = true;
                $scope.createWorkSheetTables(table);
            });
            
    }
/* Todo Implement the json logic
$scope.jsonSchema;
$scope.tableList = [];
    $scope.createJsonForSchema = function(tableName){
         $scope.tableList.push(key);
        if( $scope.tableList.length == 1 ){
            $scope.tableList.push(tableName);
            $scope.jsonSchema = {"table": {"name" : tableName}}
        }else if( $scope.tableList.length > 1 ){
            angular.forEach($scope.myTable.selected, function(value,key) {
                if(value){
                    $scope.jsonSchema = {"table": {"name" : key,
                                                     "joinlist": [{"table" :{
                                                                    "name" : "B"                                                                                                                         
                                                                }
                                                            }                                                                                
                                                 ]
                                                }
                }
                }
            })
        }
        
    } */
    $scope.toggleTableSelection = function(tableName, click){
        var found = Object.keys($scope.myTable.selected).filter(function(key) {
            return $scope.myTable.selected[tableName];
          });
        if(!found.length){
            $scope.myTable.selected[tableName] = true;
        }else{
            /* $scope.workSheetTables.push(tempObj); */
            $scope.myTable.selected[tableName] = false;
            for(var i = $scope.workSheetTables.length - 1; i >= 0; i--){
                if($scope.workSheetTables[i].name == tableName){
                    $scope.workSheetTables.splice(i,1);
                }
            }
        }
    }

    $scope.createWorkSheetTables = function(table){
        if( $scope.isRowRes && $scope.isColumnRes){
            if($scope.workSheetTables.indexOf(table) == -1){
                var tempObj = {
                    rowCount: $scope.rowCount,
                    colCount: $scope.columnList.length,
                    name: table
                }
                var isPresent = $scope.workSheetTables.some(function(el){ return el.name === table});
                    if(!isPresent){
                        $scope.workSheetTables.push(tempObj);
                        $scope.toggleTableSelection(table);
                    }else{
                        $scope.toggleTableSelection(table);
                    }
                $scope.isRowRes = false;
                $scope.isColumnRes = false;
            }
        }
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
$scope.colsForSelection =[]
    $scope.updateColumnList = function(){
        $scope.colsForSelection =[]
        angular.forEach($scope.columnList, function(key) {
            if($scope.colsForSelection.indexOf(key.name) == -1) {
                if($scope.selectedCols.indexOf(key.name) == -1) {
                    $scope.colsForSelection.push(key.name)
                }
            }
        })
    }
    
    $scope.addColumn = function(column){
        if($scope.selectedCols.indexOf(column) == -1) {
            $scope.selectedCols.push(column);
            var index = $scope.colsForSelection.indexOf(column);
            $scope.colsForSelection.splice(index, 1);   
        }
    }
    $scope.selectedColumns = {
        selected:{}
    };
    $scope.removeColumn = function(column){
        if($scope.colsForSelection.indexOf(column) == -1) {
            $scope.colsForSelection.push(column);
            var index = $scope.selectedCols.indexOf(column);
            $scope.selectedCols.splice(index, 1);  
        }
    }


}])