app.controller('dashboardController', ['$scope', '$rootScope', '$http', 'ApiFactory', '$stateParams', 'NgTableParams', function($scope, $rootScope, $http, ApiFactory, $stateParams, NgTableParams) {
  
    $scope.myTable = {
        selected:{}
    };
    $scope.temp = [];
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
    $scope.isColumnSelected = false;
    $scope.tableList = [];
    $scope.influncerTab = false;
    $scope.influncer = false;
    $scope.worksheetTab = true;
    $scope.resize = false;
    $scope.colsForSelection = [];
    $scope.deepExplanationTabLink = false;
    $scope.deepExplanationTab = false;
    $scope.deepExplanation = false;
    $rootScope.loading =  false;
    $scope.rareEvent = {
        val: 0.01
    }
    $scope.riskRatio = {
        val: 1.8
    }
    $scope.metricCols = [];
    $scope.kpiData = [];
    $scope.minRareEvent = 0.005;
    $scope.maxRareEvent = 0.1;
    $scope.stepRareEvent = 0.001;

    $scope.minRiskRatio = 1.5;
    $scope.maxRiskRatio = 10;
    $scope.stepRiskRatio = 0.1;

    $scope.outlierFilter = '';
    $scope.objective = '';

    $scope.init = function(){
        $scope.hideLoader(); 
    }

    $scope.showLoader = function(){
        $rootScope.loading = true;
        $('body').addClass( "loadingScreen" );
    }

    $scope.hideLoader = function(){
        $rootScope.loading = false;
        $('body').removeClass( "loadingScreen" );
    }
    $scope.dragOptions = {
        /* start: function(e) {
        },
        drag: function(e) {
        },
        stop: function(e) {
        }, */
        container: 'worksheet'
    }
    
    $scope.refreshTables = function(){
        $scope.showLoader();
        ApiFactory.refreshTables.get({
        }, function (response) {
            $scope.hideLoader();
             $scope.tables = response.results;
        })
    }
    
    $scope.getData = function(table,dragged) {
           $scope.showLoader();
           $rootScope.loading = true;
           $scope.tableName = table;
           $scope.clearColumnSelection();
           $scope.worksheet=true;
   
          $scope.createJsonForSchema(table);
          if($scope.tableList.length > 0){
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
                    $scope.hideLoader();
                }, function(err){
                    $scope.hideLoader();
                });
            }, function(err){
                $scope.hideLoader();
            });
        }else{
            $scope.toggleTableSelection(table);
            $scope.hideLoader();
        }
            
           

            
            
    }
// Todo Implement the json logic

    $scope.applyOutlier = function(){
        $('#outlierModel').modal('hide');
    }

    $scope.createJsonForSchema = function(tableName){
       
        if($scope.tableList.indexOf(tableName) == -1) {
            $scope.tableList.push(tableName);
          }else{
            var index = $scope.tableList.indexOf(tableName);
            $scope.tableList.splice(index, 1);
          }
        var tables = $scope.tableList;
       
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
        $scope.showLoader();
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
                        $scope.isColumnSelected = false;
                        $scope.tableList =[];
                    }
                }
            }
        }
        $scope.hideLoader();
    }

    $scope.createWorkSheetTables = function(table){
        $scope.showLoader();
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
        $scope.hideLoader();
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
            if($scope.selectedCols.indexOf(columnName) == -1) {
                $scope.selectedCols.push(columnName);
                $scope.metricCols.push(columnName);
                //var sel = document.querySelectorAll('col#'+columnName)[0].className='active';
              }else{
                var index = $scope.selectedCols.indexOf(columnName);
                $scope.selectedCols.splice(index, 1);
                $scope.metricCols.splice(index, 1);
                //var sel = document.querySelectorAll('col#'+columnName)[0].classList.remove('active');
              }
           if($scope.selectedCols.length > 0){
                $scope.isColumnSelected =true;
           }else{
                $scope.isColumnSelected =false;
           }
    }
        
    $scope.clearColumnSelection = function(){
        $scope.selectedCols =[];
    }

    $scope.closeTab = function (tabName){
        $scope.showLoader();
        if(tabName == 'influncer'){
            $scope.influncerTab = false;
            $scope.influncer = false;
            $scope.worksheetTab = true;
            $('#tabId li.active').removeClass('active');
            $('.worksheetClass').addClass('active');
            $scope.hideLoader();
        }
        else{
            $scope.deepExplanationTab = false;
            $scope.deepExplanationTabLink = false;
            $scope.worksheetTab = true;
            $('#tabId li.active').removeClass('active');
            $('.worksheetClass').addClass('active');
            $scope.hideLoader();
        }
    }

    $scope.goToTab = function(tabName){
        $scope.showLoader();
        if(tabName == 'worksheet'){
            $scope.influncerTab=false;
            $scope.deepExplanationTab=false;
            $scope.worksheetTab=true;
            $('#tabId li.active').removeClass('active');
            $('.worksheetClass').addClass('active');
            $scope.hideLoader();
        }else if(tabName == 'influncer'){
            $('#tabId li.active').removeClass('active');
            $('.influcerClass').addClass('active');
            $scope.worksheetTab=false;
            $scope.deepExplanationTab=false;
            $scope.influncerTab=true;
            $scope.influncer=true;      
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
            $scope.hideLoader();
        }else if(tabName == 'deepExplanation'){
            $scope.influncerTab=false;
            $scope.worksheetTab=false;
            $scope.deepExplanationTab =true;
            $scope.deepExplanationTabLink=true;
            $('#myModal').modal('hide');
            $scope.getDeepExplaination();
            $('#tabId li.active').removeClass('active');
            $('.deepClass').addClass('active');
            $scope.hideLoader();
        }

    }
    $scope.deepExplanationList = [];
    $scope.deepExplanationHeader ='';
    $scope.getDeepExplaination = function(){
        $scope.showLoader();
        var startTime = new Date().getTime();
        ApiFactory.deepInsight.save({
            "workflowid": $scope.workflowid,
            "metric": $scope.metricCols[0],
            "objective":$scope.objective == '' ? 'defult' :$scope.objective,
            "optionalConf":{
                "attributes": $scope.kpiData,
                "predicate":$scope.outlierFilter == '' ? undefined : $scope.outlierFilter,
                "minSupport":$scope.rareEvent.val,
                "minRatioMetric": $scope.riskRatio.val
            }
        }, function (response) {
            $scope.respTime = (new Date().getTime() - startTime) / 1000;
            $scope.deepExplanationList = response.expl.nlgExplanation;
            $scope.deepExplanationHeader = response.expl.header;
            $scope.hideLoader();
        })
    }

    $scope.runDeepExplaination = function(data, id, cid){
        $scope.showLoader();
        var graphId = 'graph' + id + '_' + cid;
       /*  var scollable = '#scroll' + id + cid; */
        var data = eval(data);
        if(data.p.graphType == 'area'){
           /*  $(scollable).resizable({
                stop: function( event, ui ) { 
                    $scope.resize= true;
                    $scope.addAreaChart(data, graphId);
                }
            }); */
            $scope.addAreaChart(data, graphId);
        }
        else if(data.p.graphType == 'bar'){
            $scope.addBarChart(data, graphId);
        } 
        else if(data.p.graphType == 'line'){
            $scope.addLineChart(data, graphId);
        }
        $scope.hideLoader();
    }

    $scope.editPopup = function(val){
        $scope.showLoader();
        $('#myModal').modal('show');
        if(val == 'deepExplanation'){
            $scope.deepExplanation = true;
        }else{
            $scope.deepExplanation = false;
        }
        $scope.hideLoader();
    }

    $scope.applyRareEventRange = function(){
        $scope.showLoader();
        $scope.minRareEvent =$scope.minRareEventEdit;
        $scope.maxRareEvent =$scope.maxRareEventEdit
        $('#rareEventModel').modal('hide');
        $scope.hideLoader();
    }

    $scope.applyRiskRatioRange = function(){
        $scope.showLoader();
        $scope.minRiskRatio =$scope.minRiskRatioEdit;
        $scope.maxRiskRatio =$scope.maxRiskRatioEdit
        $('#riskRationModel').modal('hide');
        $scope.hideLoader();
    }

    $scope.updateColumnList = function(btn){
        $scope.showLoader();
        if(btn == 'deepExplanation'){
            $scope.deepExplanation = true;
        }else{
            $scope.deepExplanation = false;
        }
        $scope.colsForSelection =[]
        angular.forEach($scope.columnList, function(key) {
            if($scope.colsForSelection.indexOf(key.name) == -1) {
                if($scope.selectedCols.indexOf(key.name) == -1) {
                    $scope.colsForSelection.push(key.name);
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
            $scope.kpiData =[];
            if(response.kpidata[0].pearsonfeatures != undefined && response.kpidata[0].pearsonfeatures != null && response.kpidata[0].pearsonfeatures.length > 0){
                angular.forEach(response.kpidata[0].pearsonfeatures, function(key, name) {
                    $scope.kpiData.push(key.predictorname);
                })
            }
            if(response.kpidata[0].chisquarefeatures != undefined && response.kpidata[0].chisquarefeatures != null && response.kpidata[0].chisquarefeatures.length > 0){
                angular.forEach(response.kpidata[0].chisquarefeatures, function(key, name) {
                    $scope.kpiData.push(key.predictorname);
                })
            }
            if(response.kpidata[0].anovafeatures != undefined && response.kpidata[0].anovafeatures != null && response.kpidata[0].anovafeatures.length > 0){
                angular.forEach(response.kpidata[0].anovafeatures, function(key, name) {
                    $scope.kpiData.push(key.predictorname);
                })
            }
            $scope.colsForSelection =[]
            angular.forEach($scope.columnList, function(key) {
                if($scope.colsForSelection.indexOf(key.name) == -1) {
                    if($scope.selectedCols.indexOf(key.name) == -1 && $scope.kpiData.indexOf(key.name) == -1) {
                        $scope.colsForSelection.push(key.name);
                    }
                }
            })
             $scope.hideLoader();
        }, function(err){
            $scope.hideLoader();
        })
    }

    
    $scope.addColumn = function(column){
        $scope.showLoader();
        if($scope.kpiData.indexOf(column) == -1) {
            $scope.kpiData.push(column);
            var index = $scope.colsForSelection.indexOf(column);
            $scope.colsForSelection.splice(index, 1); 
            $scope.hideLoader();  
        }
    }

    $scope.selectedColumns = {
        selected:{}
    };

    $scope.removeColumn = function(column){
        $scope.showLoader();
        if($scope.colsForSelection.indexOf(column) == -1) {
            $scope.colsForSelection.push(column);
            var index = $scope.kpiData.indexOf(column);
            $scope.kpiData.splice(index, 1);  
            $scope.hideLoader();
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
    /* we may not use
    $scope.compileChartData = function(data){
        $scope.compiledData = [];
        $scope.compiledData.push([data.features[0], $scope.metricCols[0]]);
        angular.forEach(data.graphs[0].dataPoints, function(key,value) {
                $scope.compiledData.push([key.feature, parseFloat(key.metric)]);
        });
    } */

    $scope.addBarChart = function(deepData,id){
        if($scope.resize){
            drawChart();
            $scope.resize = false;
        }
        if(deepData){
           // $scope.compileChartData(deepData);
            $scope.rawChartDate = deepData;
        }
        google.charts.load('current', {'packages':['bar']});
        //google.charts.setOnLoadCallback(drawChart);
        google.setOnLoadCallback(function() { drawChart(deepData); });

        //function drawChart() {
        function drawChart(datum) {
            //var data = new google.visualization.DataTable($scope.rawChartDate,0.6);
            var data = new google.visualization.DataTable(datum,0.6);
            /* var data = google.visualization.arrayToDataTable($scope.rawChartDate); */

            var options = {
            colors:['#0BE880','#0FBCF9', '#EBAD52', '#EA4C87'],
            bars: 'vertical',
            legend: { position: 'bottom' },
            chartArea: {
                left: '8%',
                top: '10',
                bottom: '80',
                right: '0'
            }
            };
            if(deepData){
                var chart = new google.charts.Bar(document.getElementById(id));
                chart.draw(data, options);
            }else{
                var chart = new google.charts.Bar(document.getElementById('barChart'));
                chart.draw(data, google.charts.Bar.convertOptions(options));
            }
        }
    }

    $scope.addAreaChart = function(deepData, id){
        if($scope.resize){
            drawChart();
            $scope.resize = false;
        }
        if(deepData){
            //$scope.compileChartData(deepData);
            $scope.rawChartDate = deepData;
        }
        google.charts.load('current', {'packages':['corechart']});
        //google.charts.setOnLoadCallback(drawChart);
        google.setOnLoadCallback(function() { drawChart(deepData); });

        //function drawChart() {
        function drawChart(datum) {
            //var data = new google.visualization.DataTable($scope.rawChartDate,0.6);
            var data = new google.visualization.DataTable(datum,0.6);
            /* var data = google.visualization.arrayToDataTable($scope.rawChartDate); */

          var options = {
           /*  title: 'Company Performance', */
           /*  hAxis: {title: 'Year',  titleTextStyle: {color: '#333'}},
            vAxis: {minValue: 0}, */
            colors:['#0BE880','#0FBCF9', '#EBAD52', '#EA4C87'],
            pointShape: 'circle',
            selectionMode: 'multiple',
            pointsVisible : false,
            legend: { position: 'bottom' },
            chartArea: {
                left: '8%',
                top: '10',
                bottom: '80',
                right: '0'
            }
          };
          if(deepData){
            var chart = new google.visualization.AreaChart(document.getElementById(id));
            chart.draw(data, options);
        }else{
            var chart = new google.visualization.AreaChart(document.getElementById('areaChart'));
            chart.draw(data, options);
        }

        }
    }

    $scope.addPieChart = function(deepData, id){
        if($scope.resize){
            drawChart();
            $scope.resize = false;
        }
        google.charts.load('current', {'packages':['corechart']});
        google.charts.setOnLoadCallback(drawChart);
        if(deepData){
            //$scope.compileChartData(deepData);
            $scope.rawChartDate = deepData;
        }
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
            legend: { position: 'bottom' },
            chartArea: {
                left: '8%',
                top: '10',
                bottom: '80',
                right: '0'
            }
          };
          if(deepData){
            var chart = new google.visualization.PieChart(document.getElementById(id));
            chart.draw(data, options);
        }else{
            var chart = new google.visualization.PieChart(document.getElementById('pieChart'));
          chart.draw(data, options);
        }
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
        var data = new google.visualization.DataTable($scope.rawChartDate);

        var options = {
        /*   title: 'Company Performance', */
          curveType: 'function',
          colors:['#0BE880','#0FBCF9', '#EBAD52', '#EA4C87'],
          legend: { position: 'bottom' },
          chartArea: {
            left: '8%',
            top: '10',
            bottom: '80',
            right: '0'
        }
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
  
        var data = new google.visualization.DataTable([
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
          chartArea: {
            left: '8%',
            top: '10',
            bottom: '80',
            right: '0'
        }
        };
  
        var chart = new google.visualization.BubbleChart(document.getElementById('bubbleChart'));
        chart.draw(data, options);
      }
    }
}])