app.controller('dashboardController', ['$scope', '$http', 'ApiFactory', '$stateParams', 'NgTableParams', function($scope, $http, ApiFactory, $stateParams, NgTableParams) {

    /* $scope.ApiFactory = {
        schema: {"columns":[{"name":"aa"},{"type":"ass"}]}
    } */
    
    $scope.myTable = {
        selected:{}
    };
    $scope.temp= [];
    $scope.schemaCols = {};
    $scope.selectedCols = [];
    $scope.workSheetTables = [];
    $scope.columnList = {};
    $scope.schemaRows = {};
    $scope.rowCount = 0;
    $scope.tableNames  = $stateParams.table;
    $scope.isColumnRes = false;
    $scope.isRowRes = false;
    $scope.workflowid;
    $scope.isColumnSelected =false;
    $scope.tableList = [];
    $scope.influncerTab=false;
    $scope.influncer=false;
    $scope.worksheetTab=true;
    $scope.resize= false;
    $scope.colsForSelection =[];
    $scope.deepExplanation = false;

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
           $scope.worksheet=true;
          var temp =[];
          $scope.createJsonForSchema(table);
            ApiFactory.schema.save({
                table: $scope.jsonSchema
            }, function (response) {
                $scope.columnList = response.schema.columns;
                $scope.workflowid = response.workflowid;
                var newJson=[]
                angular.forEach(response.schema.columns, function(value,key) {
                    var name =value.name
                    
                    var abc = {
                        "name": name,
                        "type": value.type,
                        "title": name,
                        "sortable": name,
                        "filter": {
                            name: value.type =='INTEGER' ?'number' : "text"
                        },
                        "show": true,
                        "field":name
                    }
                    newJson.push(abc);
                    $scope.isColumnRes = true;
                })
                $scope.schemaCols = newJson;
                $scope.createWorkSheetTables(table);
                ApiFactory.getRows.save({
                    workflowid: $scope.workflowid
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
            });

           

            
            
    }
// Todo Implement the json logic

    $scope.createJsonForSchema = function(tableName){
       
        if($scope.tableList.indexOf(tableName) == -1) {
            $scope.tableList.push(tableName);
          }else{
            var index = $scope.tableList.indexOf(tableName);
            $scope.tableList.splice(index, 1);
          }
        var tables= $scope.tableList;
       
        //tables = ['test1', 'test2', 'test3', 'test4'];
        var currentTable = null;
        for (var i = tables.length - 1; i >= 0; --i) {
          if (currentTable == null && tables.length ==1) {
            currentTable = {name: tables[i]};
          }else if(currentTable == null && tables.length > 1){
            currentTable = {table : {name: tables[i]}};
          } else {
           var newTable =  {name: tables[i], joinlist : [currentTable]};
           currentTable = newTable;
          }
        }
         $scope.jsonSchema = currentTable;/* JSON.stringify(currentTable); */
        
    } 
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
                    if($scope.workSheetTables.length == 0){
                        $scope.schemaCols = {};
                    }
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

    $scope.toggleColumnSelection = function(columnName, index){
        console.log(index)
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
           if($scope.selectedCols.length > 0){
                $scope.isColumnSelected =true;
           }else{
                $scope.isColumnSelected =false;
           }
    }
        
    $scope.clearColumnSelection = function(){
        $scope.selectedCols =[];
    }

    $scope.goToTab = function(tabName){
        if(tabName == 'worksheet'){
            $scope.influncerTab=false;
            $scope.worksheetTab=true;
        }else if(tabName == 'influncer'){
            $('#influncer').tab('show');
            $scope.influncerTab=true;
            $scope.influncer=true;
            $scope.worksheetTab=false;
            $('#myModal').modal('hide');
            $('#barChartContainer').resizable({
                stop: function( event, ui ) { 
                    $scope.resize= true;
                    $scope.addBarChart();
                }
            });
            $('#areaChartContainer').resizable({
                stop: function( event, ui ) { 
                    $scope.resize= true;
                    $scope.addAreaChart();
                }
            });
            $('#lineChartContainer').resizable({
                stop: function( event, ui ) { 
                    $scope.resize= true;
                    $scope.addLineChart();
                }
            });
            $('#pieChartContainer').resizable({
                stop: function( event, ui ) { 
                    $scope.resize= true;
                    $scope.addPieChart();
                }
            });
            $('#bubbleChartContainer').resizable({
                stop: function( event, ui ) { 
                    $scope.resize= true;
                    $scope.addBubbleChart();
                }
            });
            
            $scope.addAreaChart();
            $scope.addBarChart();
            $scope.addLineChart();
            $scope.addPieChart();
            $scope.addBubbleChart();
           
        }else if(tabName == 'deepExplanation'){
            $scope.influncerTab=false;
            $scope.worksheetTab=true;
        }

    }
    $scope.editPopup = function(){
        $('#myModal').modal('show')
    }

    
    $scope.updateColumnList = function(btn){
        if(btn == 'deepExplanation'){
            $scope.deepExplanation = true;
        }else{
            $scope.deepExplanation = false;
        }
        $scope.colsForSelection =[]
        angular.forEach($scope.columnList, function(key) {
            if($scope.colsForSelection.indexOf(key.name) == -1) {
                if($scope.selectedCols.indexOf(key.name) == -1) {
                    $scope.colsForSelection.push(key.name)
                }
            }
        })
       // $scope.selectedCols =[];
        ApiFactory.getInsight.save({
            "workflowid": $scope.workflowid,
            "kpicols": $scope.selectedCols
        }, function (response) {
            $('#myModal').modal('show')
           /*  $('#myModal').modal('toggle') */
            $scope.selectedCols =[];
        if(response.kpidata[0].pearsonfeatures != undefined && response.kpidata[0].pearsonfeatures != null && response.kpidata[0].pearsonfeatures.length > 0){
            angular.forEach(response.kpidata[0].pearsonfeatures, function(key, name) {
                $scope.selectedCols.push(key.predictorname);
            })
        }
        if(response.kpidata[0].chisquarefeatures != undefined && response.kpidata[0].chisquarefeatures != null && response.kpidata[0].chisquarefeatures.length > 0){
            angular.forEach(response.kpidata[0].chisquarefeatures, function(key, name) {
                $scope.selectedCols.push(key.predictorname);
            })
        }
        if(response.kpidata[0].anovafeatures != undefined && response.kpidata[0].anovafeatures != null && response.kpidata[0].anovafeatures.length > 0){
            angular.forEach(response.kpidata[0].anovafeatures, function(key, name) {
                $scope.selectedCols.push(key.predictorname);
            })
        }
        console.log(response.kpidata[0]);
             /* $scope.tables = response.results; */
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
    
$scope.rawChartDate = [
    ['Year', 'Sales', 'Expenses', 'Profit'],
      ['2014', 1000, 400, 200],
      ['2015', 1170, 460, 250],
      ['2016', 660, 1120, 300],
      ['2017', 1030, 540, 350],
      ['2018', 545, 222, 253],
      ['2019', 312, 540, 444],
      ['2020', 700, 544, 222],
      ['2021', 921, 440, 150],
      ['2022', 880, 510, 310],
      ['2023', 400, 880, 450],
      ['2024', 250, 230, 310],
      ['2025', 180, 450, 350],
      ['2026', 150, 580, 312]
  ]
$scope.addBarChart = function(){
    if($scope.resize){
        drawChart();
        $scope.resize = false;
    }
    google.charts.load('current', {'packages':['bar']});
      google.charts.setOnLoadCallback(drawChart);

      function drawChart() {
        var data = google.visualization.arrayToDataTable($scope.rawChartDate);

        var options = {
          colors:['#0BE880','#0FBCF9', '#EBAD52', '#EA4C87'],
          bars: 'horizontal',
          legend: { position: 'bottom' }
        };

        var chart = new google.charts.Bar(document.getElementById('barChart'));

        chart.draw(data, google.charts.Bar.convertOptions(options));
      }
    }

    $scope.addAreaChart = function(){
        if($scope.resize){
            drawChart();
            $scope.resize = false;
        }
        google.charts.load('current', {'packages':['corechart']});
        google.charts.setOnLoadCallback(drawChart);
  
        function drawChart() {
          var data = google.visualization.arrayToDataTable($scope.rawChartDate);
  
          var options = {
           /*  title: 'Company Performance', */
            hAxis: {title: 'Year',  titleTextStyle: {color: '#333'}},
            vAxis: {minValue: 0},
            colors:['#0BE880','#0FBCF9', '#EBAD52', '#EA4C87'],
            pointShape: 'circle',
            selectionMode: 'multiple',
            pointsVisible : false,
            legend: { position: 'bottom' },
          };
  
          var chart = new google.visualization.AreaChart(document.getElementById('areaChart'));
          chart.draw(data, options);
        }
    }

    $scope.addPieChart = function(){

        if($scope.resize){
            drawChart();
            $scope.resize = false;
        }
        google.charts.load('current', {'packages':['corechart']});
        google.charts.setOnLoadCallback(drawChart);
  
        function drawChart() {
  
          var data = google.visualization.arrayToDataTable([
            ['Task', 'Hours per Day'],
            ['Work',     14],
            ['Eat',      1],
            ['Commute',  2],
            ['Watch TV', 1],
            ['Sleep',    6]
          ]);
  
          var options = {
           /*  title: 'My Daily Activities', */
            colors:['#0BE880','#0FBCF9', '#EBAD52', '#EA4C87'],
            legend: { position: 'bottom' }
          };
  
          var chart = new google.visualization.PieChart(document.getElementById('pieChart'));
  
          chart.draw(data, options);
        }
    }

    $scope.addLineChart = function (){
        if($scope.resize){
            drawChart();
            $scope.resize = false;
        }
        google.charts.load('current', {'packages':['corechart']});
      google.charts.setOnLoadCallback(drawChart);

      function drawChart() {
        var data = google.visualization.arrayToDataTable($scope.rawChartDate);

        var options = {
        /*   title: 'Company Performance', */
          curveType: 'function',
          colors:['#0BE880','#0FBCF9', '#EBAD52', '#EA4C87'],
          legend: { position: 'bottom' }
        };

        var chart = new google.visualization.LineChart(document.getElementById('lineChart'));

        chart.draw(data, options);
      }
    }

    $scope.addBubbleChart = function(){
        if($scope.resize){
            drawSeriesChart();
            $scope.resize = false;
        }
        google.charts.load('current', {'packages':['corechart']});
        google.charts.setOnLoadCallback(drawSeriesChart);
  
      function drawSeriesChart() {
  
        var data = google.visualization.arrayToDataTable([
          ['ID', 'Life Expectancy', 'Fertility Rate', 'Region',     'Population'],
          ['CAN',    80.66,              1.67,      'North America',  33739900],
          ['DEU',    79.84,              1.36,      'Europe',         81902307],
          ['DNK',    78.6,               1.84,      'Europe',         5523095],
          ['EGY',    72.73,              2.78,      'Middle East',    79716203],
          ['GBR',    80.05,              2,         'Europe',         61801570],
          ['IRN',    72.49,              1.7,       'Middle East',    73137148],
          ['IRQ',    68.09,              4.77,      'Middle East',    31090763],
          ['ISR',    81.55,              2.96,      'Middle East',    7485600],
          ['RUS',    68.6,               1.54,      'Europe',         141850000],
          ['USA',    78.09,              2.05,      'North America',  307007000]
        ]);
  
        var options = {
        /*   title: 'Correlation between life expectancy, fertility rate ' +
                 'and population of some world countries (2010)', */
          hAxis: {title: 'Life Expectancy'},
          vAxis: {title: 'Fertility Rate'},
          bubble: {textStyle: {fontSize: 10}},
          colors:['#0BE880','#0FBCF9', '#EBAD52', '#EA4C87'],
        };
  
        var chart = new google.visualization.BubbleChart(document.getElementById('bubbleChart'));
        chart.draw(data, options);
      }
    }
}])