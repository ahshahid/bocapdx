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

           $scope.createJsonForSchema(table);

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
// Todo Implement the json logic
$scope.tableList = [];
    $scope.createJsonForSchema = function(tableName){
        tables = ['test1', 'test2', 'test3', 'test4'];
        var currentTable = null;
        for (var i = tables.length - 1; i >= 0; --i) {
          if (currentTable == null) {
            currentTable = {table : {name: tables[i]}};
          } else {
           var newTable = {table : {name: tables[i], joinlist : [currentTable]}};
           currentTable = newTable;
          }
        }
        console.log(currentTable);
        $scope.jsonSchema = JSON.stringify(currentTable);
        
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
$scope.influncerTab=false;
$scope.influncer=false;
$scope.worksheetTab=true;
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
            var chart = $("#chart"),
    aspect = chart.width() / chart.height(),
    container = chart.parent();
            $('#resizable').resizable({
                resize: function( event, ui ) {
                    var targetWidth = container.width();
                    chart.attr("width", targetWidth);
                    chart.attr("height", Math.round(targetWidth / aspect));
                }
            });
            $('#resizable1').resizable();
            $scope.addBarChart();
           
        }

    }
    $scope.editPopup = function(){
        $('#myModal').modal('show')
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
    



$scope.addBarChart = function(){
    var margin = {top: 25, right: 75, bottom: 85, left: 85},
				w = 400 - margin.left - margin.right,
				h = 350 - margin.top - margin.bottom;
var padding = 10;

var colors =	[["Local", "#377EB8"],
				 ["Global", "#4DAF4A"]];

var dataset = [
				{"keyword": "Q1", "global": 40000, "local": 73000, "cpc": "14.11"},
				{"keyword": "Q2", "global": 165000, "local": 160000, "cpc": "12.53" },
				{"keyword": "Q3", "global": 50000, "local": 101000, "cpc": "6.14"},
				{"keyword": "Q4", "global": 15400, "local": 12900, "cpc": "5.84"},
				{"keyword": "Q4", "global": 15400, "local": 12900, "cpc": "5.84"},
				{"keyword": "Q4", "global": 15400, "local": 12900, "cpc": "5.84"},
				{"keyword": "Q4", "global": 15400, "local": 12900, "cpc": "5.84"},
				{"keyword": "Q4", "global": 15400, "local": 12900, "cpc": "5.84"},
				{"keyword": "Q4", "global": 15400, "local": 12900, "cpc": "5.84"},
				{"keyword": "Q4", "global": 15400, "local": 12900, "cpc": "5.84"},
				{"keyword": "Q4", "global": 15400, "local": 12900, "cpc": "5.84"},
				{"keyword": "Q5", "global": 111600, "local": 11500, "cpc": "11.74"}
			];

var xScale = d3.scale.ordinal()
				.domain(d3.range(dataset.length))
				.rangeRoundBands([0, w], 0.05); 
// ternary operator to determine if global or local has a larger scale
var yScale = d3.scale.linear()
				.domain([0, d3.max(dataset, function(d) { return (d.local > d.global) ? d.local : d.global;})]) 
				.range([h, 0]);
var xAxis = d3.svg.axis()
				.scale(xScale)
				.tickFormat(function(d) { return dataset[d].keyword; })
				.orient("bottom");
var yAxis = d3.svg.axis()
				.scale(yScale)
				.orient("left")
				.ticks(5);

var commaFormat = d3.format(',');

//SVG element
var svg = d3.select("#searchVolume")
			.append("svg")
			.attr("width", w + margin.left + margin.right)
			.attr("height", h + margin.top + margin.bottom)
			.append("g")
			.attr("transform", "translate(" + margin.left + "," + margin.top + ")");
	
// Graph Bars
var sets = svg.selectAll(".set") 
	.data(dataset) 
	.enter()
	.append("g")
    .attr("class","set")
    .attr("transform",function(d,i){
         return "translate(" + xScale(i) + ",0)";
     })	;

sets.append("rect")
    .attr("class","local")
	.attr("width", xScale.rangeBand()/2)
	.attr("y", function(d) {
		return yScale(d.local);
	})
    .attr("x", xScale.rangeBand()/2)
    .attr("height", function(d){
        return h - yScale(d.local);
    })
	.attr("fill", colors[0][1])
	.on("mouseover", function(d,i) {
		//Get this bar's x/y values, then augment for the tooltip
		var xPosition = parseFloat(xScale(i) + xScale.rangeBand() );
		var yPosition = h / 2;
		//Update Tooltip Position & value
		d3.select("#tooltip")
			.style("left", xPosition + "px")
			.style("top", yPosition + "px")
			.select("#cpcVal")
			.text(d.cpc);
		d3.select("#tooltip")
			.select("#volVal")
			.text(commaFormat(d.local));
		d3.select("#tooltip")
			.select("#keyword")
			.style("color", colors[1][1])
			.text(d.keyword);
		d3.select("#tooltip").classed("hidden", false);
	})
	.on("mouseout", function() {
		//Remove the tooltip
		d3.select("#tooltip").classed("hidden", true);
	})
   	;

sets.append("rect")
    .attr("class","global")
	.attr("width", xScale.rangeBand()/2)
	.attr("y", function(d) {
		return yScale(d.global);
	})
    .attr("height", function(d){
        return h - yScale(d.global);
    })
	.attr("fill", colors[1][1])
	.on("mouseover", function(d,i) {
		//Get this bar's x/y values, then augment for the tooltip
		var xPosition = parseFloat(xScale(i) + xScale.rangeBand() );
		var yPosition = h / 2;
		//Update Tooltip Position & value
		d3.select("#tooltip")
			.style("left", xPosition + "px")
			.style("top", yPosition + "px")
			.select("#cpcVal")
			.text(d.cpc);
		d3.select("#tooltip")
			.select("#volVal")
			.text(commaFormat(d.global));
		d3.select("#tooltip")
			.select("#keyword")
			.style("color", colors[1][1])
			.text(d.keyword);
		d3.select("#tooltip").classed("hidden", false);
	})
	.on("mouseout", function() {
		//Remove the tooltip
		d3.select("#tooltip").classed("hidden", true);
	})
	;
	
// Labels
sets.append("text")
	.attr("class", "local")
	.attr("width", xScale.rangeBand()/2)
	.attr("y", function(d) {
		return yScale(d.local);
    })
    .attr("dy", 10)
    .attr("dx", (xScale.rangeBand()/1.60) )
//	.attr("text-anchor", "middle")
    .attr("font-family", "sans-serif") 
    .attr("font-size", "8px")
    .attr("fill", "white")
	.text(function(d) {
		return commaFormat(d.local);
    });		
	
sets.append("text")
	.attr("class", "global")
	.attr("y", function(d) {
		return yScale(d.global);
    })
    .attr("dy", 10)
    .attr("dx",(xScale.rangeBand() / 4) - 10)
//	.attr("text-anchor", "middle")
    .attr("font-family", "sans-serif") 
    .attr("font-size", "8px")
    .attr("fill", "white")
	.text(function(d) {
		return commaFormat(d.global);
    });

// xAxis
svg.append("g") // Add the X Axis
	.attr("class", "x axis")
	.attr("transform", "translate(0," + (h) + ")")
	.call(xAxis)
	.selectAll("text")
		.style("text-anchor", "end")
		.attr("dx", "-.8em")
		.attr("dy", ".15em")
		.attr("transform", function(d) {
				return "rotate(-25)";
		})
		;
// yAxis
svg.append("g")
	.attr("class", "y axis")
	.attr("transform", "translate(0 ,0)")
	.call(yAxis)
	;
// xAxis label
svg.append("text") 
	.attr("transform", "translate(" + (w / 2) + " ," + (h + margin.bottom - 5) +")")
	.style("text-anchor", "middle")
	.text("Keyword");
//yAxis label
svg.append("text")
		.attr("transform", "rotate(-90)")
		.attr("y", 0 - margin.left)
		.attr("x", 0 - (h / 2))
		.attr("dy", "1em")
		.style("text-anchor", "middle")
		.text("# of Searches");

// Title
svg.append("text")
		.attr("x", (w / 2))
		.attr("y", 0 - (margin.top / 2))
		.attr("text-anchor", "middle")
		.style("font-size", "16px")
		.style("text-decoration", "underline")
		.text("Global & Local Searches");

// add legend   
var legend = svg.append("g")
		.attr("class", "legend")
//		.attr("x", w - 65)
//		.attr("y", 50)
//		.attr("height", 100)
//		.attr("width", 100)
		.attr("transform", "translate(70,10)")
		;
var legendRect = legend.selectAll('rect').data(colors);

legendRect.enter()
    .append("rect")
    .attr("x", w - 65)
//	.attr("y", 0)										// use this to flip horizontal
    .attr("width", 10)
    .attr("height", 10)
    .attr("y", function(d, i) {
        return i * 20;
    })
//	.attr("x", function(d, i){return w - 65 - i * 70}) // use this to flip horizontal
    .style("fill", function(d) {
        return d[1];
    });

var legendText = legend.selectAll('text').data(colors);

legendText.enter()
    .append("text")
    .attr("x", w - 52)
    .attr("y", function(d, i) {
        return i * 20 + 9;
    })
    .text(function(d) {
        return d[0];
    });

function updateBars()
{	
    svg.selectAll(".local").remove();
	svg.selectAll(".global").transition().duration(500).attr("width", xScale.rangeBand());
}
d3.select("#change").on("click", updateBars);
}
}])