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
    $scope.deepExplanationTabLink = false;
    $scope.deepExplanationTab =false;
    $scope.deepExplanation = false;
    $scope.rareEvent = 50;
    $scope.riskRatio = 100;

    $scope.minRareEvent =0.005;
    $scope.maxRareEvent =0.1;
    $scope.stepRareEvent =0.001;

    $scope.minRiskRatio =1.5;
    $scope.maxRiskRatio =10;
    $scope.stepRiskRatio =0.1;

    $scope.outlierFilter='';

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

    $scope.applyOutlier = function(){
        console.log($scope.outlierFilter)
        $('#outlierModel').modal('hide');
    }

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
    $scope.metricCols=[];
    $scope.toggleColumnSelection = function(columnName, index){
        //console.log(index)
            if($scope.selectedCols.indexOf(columnName) == -1) {
                $scope.selectedCols.push(columnName);
                $scope.metricCols.push(columnName);
                var sel = document.querySelectorAll('col#'+columnName)[0].className='active';
              }else{
                var index = $scope.selectedCols.indexOf(columnName);
                $scope.selectedCols.splice(index, 1);
                $scope.metricCols.splice(index, 1);
                var sel = document.querySelectorAll('col#'+columnName)[0].classList.remove('active');
              }
        //console.log($scope.selectedCols);
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
            $scope.deepExplanationTab=false;
            $scope.worksheetTab=true;
        }else if(tabName == 'influncer'){
            $('#influncer').tab('show')
            $scope.worksheetTab=false;
            $scope.deepExplanationTab=false;
            $scope.influncerTab=true;
            $scope.influncer=true;
            
            $('#myModal').modal('hide');
           /*  $('.auto-panel').resizable({
                stop: function( event, ui ) { 
                    $scope.resize= true;
                    $scope.addBarChart();
                }
            }); */
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

            $('#deepExplanation').tab('show')
            $scope.influncerTab=false;
            $scope.worksheetTab=false;
            $scope.deepExplanationTab =true;
            $scope.deepExplanationTabLink=true;
            $('#myModal').modal('hide');
            $scope.getDeepExplaination();
            
        }

    }
    $scope.deepExplanationList = [];
    $scope.getDeepExplaination = function(){
        ApiFactory.deepInsight.save({
            "workflowid": $scope.workflowid,
            "metric": $scope.metricCols[0],
            "objective":$scope.objective,
            "optionalConf":{
                "attributes": $scope.selectedCols
            }
        }, function (response) {
            $scope.deepExplanationList = response.expl.nlgExplanation
        })
    }

    $scope.runDeepExplaination = function(data, id){
       /*  $('.auto-panel').resizable(); */
        var graphId = 'graph' + id
        var scollable = '#scroll' + id;
        $scope.addAreaChart(data.graphs[0], graphId);
        /* if(data.graphs[0].graphType == 'area'){
           
            $(scollable).resizable({
                stop: function( event, ui ) { 
                    $scope.resize= true;
                    $scope.addAreaChart(data, graphId);
                }
            });
            $scope.addAreaChart(data, graphId);
        }
        else{
            $scope.addBarChart(data, graphId);
        } */
    }

    $scope.editPopup = function(val){
        $('#myModal').modal('show');
        if(val == 'deepExplanation'){
            $scope.deepExplanation = true;
        }else{
            $scope.deepExplanation = false;
        }
    }

    $scope.applyRareEventRange = function(){
        $scope.minRareEvent =$scope.minRareEventEdit;
        $scope.maxRareEvent =$scope.maxRareEventEdit
        $('#rareEventModel').modal('hide');
    }

    $scope.applyRiskRatioRange = function(){
        $scope.minRiskRatio =$scope.minRiskRatioEdit;
        $scope.maxRiskRatio =$scope.maxRiskRatioEdit
        $('#riskRationModel').modal('hide');
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
        //console.log(response.kpidata[0]);
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
    $scope.compileChartData = function(data){
        $scope.compiledData = [];
        $scope.compiledData.push([data.features[0], $scope.metricCols[0]]);
        
        angular.forEach(data.graphs[0].dataPoints, function(key,value) {
                $scope.compiledData.push([key.feature, parseFloat(key.metric)]);
        });

    }

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
        google.charts.setOnLoadCallback(drawChart);

        function drawChart() {
            var data = new google.visualization.DataTable($scope.rawChartDate,0.6);
            /* var data = google.visualization.arrayToDataTable($scope.rawChartDate); */

            var options = {
            colors:['#0BE880','#0FBCF9', '#EBAD52', '#EA4C87'],
            bars: 'horizontal',
            legend: { position: 'bottom' }
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
            $scope.rawChartDate = {cols:[{id:'feature',label:'totrev',type:'string',pattern:'',p:{'isFeatureRange':'true'}},{id:'metric',label:'avgrev',type:'number',pattern:''}],rows:[{c:[{v:'9.12 - 125.46',p:{'featureUpperBound':'- 125.46','featureLowerBound':'9.12','numElements':'192'}},{v:10.544947916666661}]},{c:[{v:'125.46 - 162.88',p:{'featureUpperBound':'- 162.88','featureLowerBound':'125.46','numElements':'212'}},{v:13.17966981132076}]},{c:[{v:'162.88 - 195.69',p:{'featureUpperBound':'- 195.69','featureLowerBound':'162.88','numElements':'209'}},{v:15.183779904306213}]},{c:[{v:'195.69 - 225.04',p:{'featureUpperBound':'- 225.04','featureLowerBound':'195.69','numElements':'208'}},{v:19.713750000000008}]},{c:[{v:'225.04 - 243.16',p:{'featureUpperBound':'- 243.16','featureLowerBound':'225.04','numElements':'153'}},{v:22.97346405228757}]},{c:[{v:'243.16 - 262.55',p:{'featureUpperBound':'- 262.55','featureLowerBound':'243.16','numElements':'206'}},{v:29.057621359223305}]},{c:[{v:'262.55 - 279.34',p:{'featureUpperBound':'- 279.34','featureLowerBound':'262.55','numElements':'205'}},{v:30.88648780487805}]},{c:[{v:'293.3 - 306.34',p:{'featureUpperBound':'- 306.34','featureLowerBound':'293.3','numElements':'200'}},{v:32.24779999999999}]},{c:[{v:'279.34 - 293.3',p:{'featureUpperBound':'- 293.3','featureLowerBound':'279.34','numElements':'202'}},{v:33.18460396039606}]},{c:[{v:'306.34 - 317.82',p:{'featureUpperBound':'- 317.82','featureLowerBound':'306.34','numElements':'200'}},{v:34.50799999999998}]},{c:[{v:'317.82 - 327.94',p:{'featureUpperBound':'- 327.94','featureLowerBound':'317.82','numElements':'196'}},{v:34.72102040816325}]},{c:[{v:'327.94 - 337.63',p:{'featureUpperBound':'- 337.63','featureLowerBound':'327.94','numElements':'196'}},{v:35.42122448979593}]},{c:[{v:'366.06 - 374.33',p:{'featureUpperBound':'- 374.33','featureLowerBound':'366.06','numElements':'188'}},{v:36.591968085106394}]},{c:[{v:'355.63 - 366.06',p:{'featureUpperBound':'- 366.06','featureLowerBound':'355.63','numElements':'239'}},{v:36.70665271966529}]},{c:[{v:'347.68 - 355.63',p:{'featureUpperBound':'- 355.63','featureLowerBound':'347.68','numElements':'192'}},{v:36.93307291666669}]},{c:[{v:'337.63 - 347.68',p:{'featureUpperBound':'- 347.68','featureLowerBound':'337.63','numElements':'194'}},{v:37.07118556701028}]},{c:[{v:'390.29 - 398.79',p:{'featureUpperBound':'- 398.79','featureLowerBound':'390.29','numElements':'230'}},{v:37.07586956521742}]},{c:[{v:'374.33 - 382.46',p:{'featureUpperBound':'- 382.46','featureLowerBound':'374.33','numElements':'188'}},{v:38.33989361702127}]},{c:[{v:'416.42 - 423.57',p:{'featureUpperBound':'- 423.57','featureLowerBound':'416.42','numElements':'180'}},{v:38.43377777777778}]},{c:[{v:'382.46 - 390.29',p:{'featureUpperBound':'- 390.29','featureLowerBound':'382.46','numElements':'186'}},{v:38.783494623655905}]},{c:[{v:'398.79 - 406.76',p:{'featureUpperBound':'- 406.76','featureLowerBound':'398.79','numElements':'184'}},{v:39.01711956521738}]},{c:[{v:'406.76 - 416.42',p:{'featureUpperBound':'- 416.42','featureLowerBound':'406.76','numElements':'225'}},{v:39.376933333333334}]},{c:[{v:'447.14 - 454.51',p:{'featureUpperBound':'- 454.51','featureLowerBound':'447.14','numElements':'215'}},{v:39.95139534883719}]},{c:[{v:'423.57 - 430.05',p:{'featureUpperBound':'- 430.05','featureLowerBound':'423.57','numElements':'178'}},{v:39.978258426966306}]},{c:[{v:'462.67 - 469.76',p:{'featureUpperBound':'- 469.76','featureLowerBound':'462.67','numElements':'170'}},{v:41.51805882352943}]},{c:[{v:'430.05 - 439.02',p:{'featureUpperBound':'- 439.02','featureLowerBound':'430.05','numElements':'220'}},{v:41.523136363636375}]},{c:[{v:'439.02 - 447.14',p:{'featureUpperBound':'- 447.14','featureLowerBound':'439.02','numElements':'176'}},{v:41.59517045454545}]},{c:[{v:'477.9 - 485.4',p:{'featureUpperBound':'- 485.4','featureLowerBound':'477.9','numElements':'210'}},{v:41.88095238095238}]},{c:[{v:'485.4 - 492.93',p:{'featureUpperBound':'- 492.93','featureLowerBound':'485.4','numElements':'205'}},{v:41.974926829268284}]},{c:[{v:'469.76 - 477.9',p:{'featureUpperBound':'- 477.9','featureLowerBound':'469.76','numElements':'210'}},{v:42.0954761904762}]},{c:[{v:'454.51 - 462.67',p:{'featureUpperBound':'- 462.67','featureLowerBound':'454.51','numElements':'215'}},{v:42.34739534883721}]},{c:[{v:'500.1 - 507.87',p:{'featureUpperBound':'- 507.87','featureLowerBound':'500.1','numElements':'202'}},{v:42.551633663366346}]},{c:[{v:'515.29 - 523.2',p:{'featureUpperBound':'- 523.2','featureLowerBound':'515.29','numElements':'201'}},{v:43.11542288557218}]},{c:[{v:'571.32 - 578.51',p:{'featureUpperBound':'- 578.51','featureLowerBound':'571.32','numElements':'185'}},{v:43.225729729729736}]},{c:[{v:'492.93 - 500.1',p:{'featureUpperBound':'- 500.1','featureLowerBound':'492.93','numElements':'205'}},{v:43.593121951219516}]},{c:[{v:'507.87 - 515.29',p:{'featureUpperBound':'- 515.29','featureLowerBound':'507.87','numElements':'199'}},{v:44.33899497487436}]},{c:[{v:'523.2 - 531.4',p:{'featureUpperBound':'- 531.4','featureLowerBound':'523.2','numElements':'195'}},{v:44.54097435897433}]},{c:[{v:'539.13 - 546.64',p:{'featureUpperBound':'- 546.64','featureLowerBound':'539.13','numElements':'193'}},{v:45.470414507772}]},{c:[{v:'587.57 - 594.75',p:{'featureUpperBound':'- 594.75','featureLowerBound':'587.57','numElements':'180'}},{v:45.628444444444426}]},{c:[{v:'554.47 - 562.57',p:{'featureUpperBound':'- 562.57','featureLowerBound':'554.47','numElements':'190'}},{v:45.6865789473684}]},{c:[{v:'531.4 - 539.13',p:{'featureUpperBound':'- 539.13','featureLowerBound':'531.4','numElements':'195'}},{v:45.85523076923078}]},{c:[{v:'578.51 - 587.57',p:{'featureUpperBound':'- 587.57','featureLowerBound':'578.51','numElements':'219'}},{v:45.89502283105025}]},{c:[{v:'653.76 - 663.74',p:{'featureUpperBound':'- 663.74','featureLowerBound':'653.76','numElements':'231'}},{v:46.160649350649344}]},{c:[{v:'594.75 - 604.62',p:{'featureUpperBound':'- 604.62','featureLowerBound':'594.75','numElements':'215'}},{v:46.36306976744186}]},{c:[{v:'546.64 - 554.47',p:{'featureUpperBound':'- 554.47','featureLowerBound':'546.64','numElements':'190'}},{v:46.453578947368406}]},{c:[{v:'663.74 - 672.41',p:{'featureUpperBound':'- 672.41','featureLowerBound':'663.74','numElements':'195'}},{v:46.60882051282049}]},{c:[{v:'690.42 - 698.22',p:{'featureUpperBound':'- 698.22','featureLowerBound':'690.42','numElements':'187'}},{v:47.30748663101604}]},{c:[{v:'612.44 - 620.95',p:{'featureUpperBound':'- 620.95','featureLowerBound':'612.44','numElements':'210'}},{v:47.383047619047595}]},{c:[{v:'737.79 - 746.92',p:{'featureUpperBound':'- 746.92','featureLowerBound':'737.79','numElements':'204'}},{v:47.63416666666667}]},{c:[{v:'620.95 - 629.25',p:{'featureUpperBound':'- 629.25','featureLowerBound':'620.95','numElements':'207'}},{v:48.24270531400963}]},{c:[{v:'672.41 - 682.13',p:{'featureUpperBound':'- 682.13','featureLowerBound':'672.41','numElements':'192'}},{v:48.33791666666667}]},{c:[{v:'562.57 - 571.32',p:{'featureUpperBound':'- 571.32','featureLowerBound':'562.57','numElements':'223'}},{v:48.37300448430495}]},{c:[{v:'604.62 - 612.44',p:{'featureUpperBound':'- 612.44','featureLowerBound':'604.62','numElements':'176'}},{v:48.518920454545444}]},{c:[{v:'682.13 - 690.42',p:{'featureUpperBound':'- 690.42','featureLowerBound':'682.13','numElements':'192'}},{v:48.83395833333332}]},{c:[{v:'629.25 - 637.38',p:{'featureUpperBound':'- 637.38','featureLowerBound':'629.25','numElements':'204'}},{v:49.21936274509803}]},{c:[{v:'644.41 - 653.76',p:{'featureUpperBound':'- 653.76','featureLowerBound':'644.41','numElements':'199'}},{v:49.274824120603014}]},{c:[{v:'698.22 - 707.82',p:{'featureUpperBound':'- 707.82','featureLowerBound':'698.22','numElements':'217'}},{v:49.574055299539204}]},{c:[{v:'746.92 - 756.12',p:{'featureUpperBound':'- 756.12','featureLowerBound':'746.92','numElements':'174'}},{v:50.83695402298854}]},{c:[{v:'637.38 - 644.41',p:{'featureUpperBound':'- 644.41','featureLowerBound':'637.38','numElements':'170'}},{v:51.026117647058825}]},{c:[{v:'786.41 - 795.35',p:{'featureUpperBound':'- 795.35','featureLowerBound':'786.41','numElements':'190'}},{v:51.49342105263157}]},{c:[{v:'716.66 - 727.07',p:{'featureUpperBound':'- 727.07','featureLowerBound':'716.66','numElements':'210'}},{v:51.56738095238095}]},{c:[{v:'727.07 - 737.79',p:{'featureUpperBound':'- 737.79','featureLowerBound':'727.07','numElements':'210'}},{v:52.298809523809496}]},{c:[{v:'774.4 - 786.41',p:{'featureUpperBound':'- 786.41','featureLowerBound':'774.4','numElements':'224'}},{v:52.30950892857142}]},{c:[{v:'707.82 - 716.66',p:{'featureUpperBound':'- 716.66','featureLowerBound':'707.82','numElements':'184'}},{v:52.437771739130405}]},{c:[{v:'836.17 - 845.89',p:{'featureUpperBound':'- 845.89','featureLowerBound':'836.17','numElements':'202'}},{v:52.678069306930674}]},{c:[{v:'756.12 - 765.35',p:{'featureUpperBound':'- 765.35','featureLowerBound':'756.12','numElements':'201'}},{v:52.89467661691545}]},{c:[{v:'765.35 - 774.4',p:{'featureUpperBound':'- 774.4','featureLowerBound':'765.35','numElements':'196'}},{v:53.12076530612246}]},{c:[{v:'804.42 - 814.71',p:{'featureUpperBound':'- 814.71','featureLowerBound':'804.42','numElements':'213'}},{v:53.34666666666669}]},{c:[{v:'814.71 - 824.97',p:{'featureUpperBound':'- 824.97','featureLowerBound':'814.71','numElements':'182'}},{v:53.790989010989016}]},{c:[{v:'824.97 - 836.17',p:{'featureUpperBound':'- 836.17','featureLowerBound':'824.97','numElements':'208'}},{v:54.07086538461537}]},{c:[{v:'926.39 - 939.63',p:{'featureUpperBound':'- 939.63','featureLowerBound':'926.39','numElements':'198'}},{v:54.308282828282806}]},{c:[{v:'939.63 - 951.77',p:{'featureUpperBound':'- 951.77','featureLowerBound':'939.63','numElements':'198'}},{v:55.40444444444445}]},{c:[{v:'962.99 - 975.76',p:{'featureUpperBound':'- 975.76','featureLowerBound':'962.99','numElements':'210'}},{v:55.45900000000002}]},{c:[{v:'795.35 - 804.42',p:{'featureUpperBound':'- 804.42','featureLowerBound':'795.35','numElements':'189'}},{v:55.66026455026457}]},{c:[{v:'845.89 - 857.65',p:{'featureUpperBound':'- 857.65','featureLowerBound':'845.89','numElements':'200'}},{v:55.81645000000001}]},{c:[{v:'880.71 - 894.04',p:{'featureUpperBound':'- 894.04','featureLowerBound':'880.71','numElements':'216'}},{v:56.078425925925956}]},{c:[{v:'904.94 - 914.82',p:{'featureUpperBound':'- 914.82','featureLowerBound':'904.94','numElements':'207'}},{v:56.32850241545896}]},{c:[{v:'894.04 - 904.94',p:{'featureUpperBound':'- 904.94','featureLowerBound':'894.04','numElements':'186'}},{v:56.47564516129033}]},{c:[{v:'988.61 - 1002.3',p:{'featureUpperBound':'- 1002.3','featureLowerBound':'988.61','numElements':'200'}},{v:56.90239999999998}]},{c:[{v:'869.41 - 880.71',p:{'featureUpperBound':'- 880.71','featureLowerBound':'869.41','numElements':'192'}},{v:57.68260416666667}]},{c:[{v:'951.77 - 962.99',p:{'featureUpperBound':'- 962.99','featureLowerBound':'951.77','numElements':'191'}},{v:57.8358115183246}]},{c:[{v:'857.65 - 869.41',p:{'featureUpperBound':'- 869.41','featureLowerBound':'857.65','numElements':'198'}},{v:58.800808080808096}]},{c:[{v:'914.82 - 926.39',p:{'featureUpperBound':'- 926.39','featureLowerBound':'914.82','numElements':'204'}},{v:59.69480392156864}]},{c:[{v:'1059.73 - 1075.41',p:{'featureUpperBound':'- 1075.41','featureLowerBound':'1059.73','numElements':'216'}},{v:60.03287037037039}]},{c:[{v:'1016.39 - 1029.2',p:{'featureUpperBound':'- 1029.2','featureLowerBound':'1016.39','numElements':'193'}},{v:60.26341968911918}]},{c:[{v:'975.76 - 988.61',p:{'featureUpperBound':'- 988.61','featureLowerBound':'975.76','numElements':'187'}},{v:60.54213903743313}]},{c:[{v:'1002.3 - 1016.39',p:{'featureUpperBound':'- 1016.39','featureLowerBound':'1002.3','numElements':'200'}},{v:60.66290000000001}]},{c:[{v:'1029.2 - 1045.18',p:{'featureUpperBound':'- 1045.18','featureLowerBound':'1029.2','numElements':'209'}},{v:61.4401913875598}]},{c:[{v:'1175.56 - 1191.21',p:{'featureUpperBound':'- 1191.21','featureLowerBound':'1175.56','numElements':'184'}},{v:61.58570652173911}]},{c:[{v:'1106.45 - 1122.13',p:{'featureUpperBound':'- 1122.13','featureLowerBound':'1106.45','numElements':'204'}},{v:61.675588235294136}]},{c:[{v:'1122.13 - 1138.89',p:{'featureUpperBound':'- 1138.89','featureLowerBound':'1122.13','numElements':'202'}},{v:62.40599009900998}]},{c:[{v:'1075.41 - 1090.26',p:{'featureUpperBound':'- 1090.26','featureLowerBound':'1075.41','numElements':'198'}},{v:63.06843434343433}]},{c:[{v:'1045.18 - 1059.73',p:{'featureUpperBound':'- 1059.73','featureLowerBound':'1045.18','numElements':'188'}},{v:63.154202127659566}]},{c:[{v:'1090.26 - 1106.45',p:{'featureUpperBound':'- 1106.45','featureLowerBound':'1090.26','numElements':'190'}},{v:63.319947368421055}]},{c:[{v:'1210.04 - 1227.81',p:{'featureUpperBound':'- 1227.81','featureLowerBound':'1210.04','numElements':'193'}},{v:64.49813471502591}]},{c:[{v:'1227.81 - 1248.8',p:{'featureUpperBound':'- 1248.8','featureLowerBound':'1227.81','numElements':'195'}},{v:64.61117948717946}]},{c:[{v:'1155.94 - 1175.56',p:{'featureUpperBound':'- 1175.56','featureLowerBound':'1155.94','numElements':'208'}},{v:64.93692307692312}]},{c:[{v:'1248.8 - 1271.28',p:{'featureUpperBound':'- 1271.28','featureLowerBound':'1248.8','numElements':'211'}},{v:64.98971563981044}]},{c:[{v:'1138.89 - 1155.94',p:{'featureUpperBound':'- 1155.94','featureLowerBound':'1138.89','numElements':'192'}},{v:65.76062499999999}]},{c:[{v:'1191.21 - 1210.04',p:{'featureUpperBound':'- 1210.04','featureLowerBound':'1191.21','numElements':'210'}},{v:66.22142857142855}]},{c:[{v:'1294.85 - 1319.03',p:{'featureUpperBound':'- 1319.03','featureLowerBound':'1294.85','numElements':'195'}},{v:66.97369230769232}]},{c:[{v:'1389.38 - 1415.03',p:{'featureUpperBound':'- 1415.03','featureLowerBound':'1389.38','numElements':'193'}},{v:67.50544041450779}]},{c:[{v:'1319.03 - 1339.58',p:{'featureUpperBound':'- 1339.58','featureLowerBound':'1319.03','numElements':'193'}},{v:69.22000000000001}]},{c:[{v:'1271.28 - 1294.85',p:{'featureUpperBound':'- 1294.85','featureLowerBound':'1271.28','numElements':'200'}},{v:69.46739999999998}]},{c:[{v:'1364.23 - 1389.38',p:{'featureUpperBound':'- 1389.38','featureLowerBound':'1364.23','numElements':'204'}},{v:69.8016176470588}]},{c:[{v:'1467.44 - 1498.08',p:{'featureUpperBound':'- 1498.08','featureLowerBound':'1467.44','numElements':'200'}},{v:70.3587}]},{c:[{v:'1339.58 - 1364.23',p:{'featureUpperBound':'- 1364.23','featureLowerBound':'1339.58','numElements':'204'}},{v:71.56357843137253}]},{c:[{v:'1438.4 - 1467.44',p:{'featureUpperBound':'- 1467.44','featureLowerBound':'1438.4','numElements':'197'}},{v:72.22517766497465}]},{c:[{v:'1415.03 - 1438.4',p:{'featureUpperBound':'- 1438.4','featureLowerBound':'1415.03','numElements':'198'}},{v:73.73065656565659}]},{c:[{v:'1498.08 - 1526.89',p:{'featureUpperBound':'- 1526.89','featureLowerBound':'1498.08','numElements':'200'}},{v:74.45849999999999}]},{c:[{v:'1557.78 - 1593.02',p:{'featureUpperBound':'- 1593.02','featureLowerBound':'1557.78','numElements':'207'}},{v:75.94985507246376}]},{c:[{v:'1593.02 - 1629.56',p:{'featureUpperBound':'- 1629.56','featureLowerBound':'1593.02','numElements':'196'}},{v:76.62382653061229}]},{c:[{v:'1666.7 - 1708.12',p:{'featureUpperBound':'- 1708.12','featureLowerBound':'1666.7','numElements':'200'}},{v:76.74830000000004}]},{c:[{v:'1526.89 - 1557.78',p:{'featureUpperBound':'- 1557.78','featureLowerBound':'1526.89','numElements':'198'}},{v:78.60242424242423}]},{c:[{v:'1629.56 - 1666.7',p:{'featureUpperBound':'- 1666.7','featureLowerBound':'1629.56','numElements':'200'}},{v:79.54715000000004}]},{c:[{v:'1708.12 - 1754.06',p:{'featureUpperBound':'- 1754.06','featureLowerBound':'1708.12','numElements':'200'}},{v:80.02825000000004}]},{c:[{v:'1895.85 - 1955.1',p:{'featureUpperBound':'- 1955.1','featureLowerBound':'1895.85','numElements':'198'}},{v:82.55636363636361}]},{c:[{v:'1796.44 - 1845.7',p:{'featureUpperBound':'- 1845.7','featureLowerBound':'1796.44','numElements':'201'}},{v:83.37044776119404}]},{c:[{v:'1754.06 - 1796.44',p:{'featureUpperBound':'- 1796.44','featureLowerBound':'1754.06','numElements':'196'}},{v:85.02607142857143}]},{c:[{v:'1845.7 - 1895.85',p:{'featureUpperBound':'- 1895.85','featureLowerBound':'1845.7','numElements':'198'}},{v:85.64070707070712}]},{c:[{v:'2016.46 - 2082.34',p:{'featureUpperBound':'- 2082.34','featureLowerBound':'2016.46','numElements':'200'}},{v:86.2075}]},{c:[{v:'2082.34 - 2161.01',p:{'featureUpperBound':'- 2161.01','featureLowerBound':'2082.34','numElements':'199'}},{v:90.09763819095485}]},{c:[{v:'1955.1 - 2016.46',p:{'featureUpperBound':'- 2016.46','featureLowerBound':'1955.1','numElements':'201'}},{v:91.06402985074625}]},{c:[{v:'2161.01 - 2260.53',p:{'featureUpperBound':'- 2260.53','featureLowerBound':'2161.01','numElements':'196'}},{v:95.39744897959186}]},{c:[{v:'2260.53 - 2372.32',p:{'featureUpperBound':'- 2372.32','featureLowerBound':'2260.53','numElements':'200'}},{v:98.78549999999997}]},{c:[{v:'2372.32 - 2491.16',p:{'featureUpperBound':'- 2491.16','featureLowerBound':'2372.32','numElements':'199'}},{v:101.66633165829148}]},{c:[{v:'2635.42 - 2807.74',p:{'featureUpperBound':'- 2807.74','featureLowerBound':'2635.42','numElements':'198'}},{v:106.45212121212114}]},{c:[{v:'2491.16 - 2635.42',p:{'featureUpperBound':'- 2635.42','featureLowerBound':'2491.16','numElements':'201'}},{v:107.83338308457705}]},{c:[{v:'2807.74 - 3066.05',p:{'featureUpperBound':'- 3066.05','featureLowerBound':'2807.74','numElements':'199'}},{v:110.66055276381915}]},{c:[{v:'3419.0 - 3896.93',p:{'featureUpperBound':'- 3896.93','featureLowerBound':'3419.0','numElements':'199'}},{v:127.36155778894471}]},{c:[{v:'3066.05 - 3419.0',p:{'featureUpperBound':'- 3419.0','featureLowerBound':'3066.05','numElements':'200'}},{v:129.23285}]},{c:[{v:'3896.93 - 4757.23',p:{'featureUpperBound':'- 4757.23','featureLowerBound':'3896.93','numElements':'199'}},{v:136.60160804020097}]},{c:[{v:'4757.23 - 13358.37',p:{'featureUpperBound':'- 13358.37','featureLowerBound':'4757.23','numElements':'226'}},{v:196.71057522123883}]}],p:{'graphType':'area'}};
        }
        google.charts.load('current', {'packages':['corechart']});
        google.charts.setOnLoadCallback(drawChart);
  
        function drawChart() {
            var data =new google.visualization.DataTable($scope.rawChartDate,0.6);
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