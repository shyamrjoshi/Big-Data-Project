
var margin = {top: 20, right: 20, bottom: 30, left: 50},
    width = 960 - margin.left - margin.right,
    height = 500 - margin.top - margin.bottom;

var formatDate = d3.time.format("%Y%m%d%H%M");

var x = d3.time.scale()
    .range([0, width])
	

var y = d3.scale.linear()
    .range([height, 0]);

var xAxis = d3.svg.axis()
    .scale(x)
    .orient("bottom")
	
	.tickFormat(d3.time.format('%X'));
    
	

var yAxis = d3.svg.axis()
    .scale(y)
    .orient("left");

var line = d3.svg.line()
    .x(function(d) { return x(d.date); })
    .y(function(d) { return y(d.revenue); });

var svg = d3.select("body").append("svg")
    .attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)
  .append("g")
    .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

d3.csv("subwayrevenue.csv", function(error, data) {
  if (error) throw error;

  data = data.map(function(d){
    //d.Subway = d.Subway,	
	d.date = formatDate.parse(d.date);
	//console.log(d.date);
    d.revenue = +d.revenue; 
    return d;
});
var mindate = d3.min(data, function(d){ return d.date; });
var maxdate = d3.max(data, function(d){ return d.date; });
//console.log(d3.time.hour.round(maxdate));
console.log(mindate);
console.log(maxdate);
  //x.domain(d3.extent(data, function(d) { return d.date; }));
  x.domain([d3.min(data, function(d){ return d.date; }), d3.max(data, function(d){ return d.date; })]);  
  y.domain([d3.min(data, function(d){ return d.revenue; }), d3.max(data, function(d){ return d.revenue; })]);

  svg.append("g")
      .attr("class", "x axis")
      .attr("transform", "translate(0," + height + ")")
      .call(xAxis)
	.append("text")
      
      .attr("x", width)
      .attr("dy", "-0.4em")
      .style("text-anchor", "end")
      .text("Day ");
	  

  svg.append("g")
      .attr("class", "y axis")
      .call(yAxis)
    .append("text")
      .attr("transform", "rotate(-90)")
      .attr("y", 6)
      .attr("dy", ".71em")
      .style("text-anchor", "end")
      .text("Revenue ($)")
	  ;

  svg.append("path")
      .datum(data)
      .attr("class", "line")
      .attr("d", line);
});



