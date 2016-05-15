var margin = {top: 50, right: 10, bottom: 30, left: 40},
    width = 860 - margin.left - margin.right,
    height = 500 - margin.top - margin.bottom;

var svg = d3.select("body")
    .append("svg")
    .attr("width", "100%")
    .attr("height", "100%")
   
var yScale = d3.scale.linear()
    .range([height - margin.top - margin.bottom, 0]);
var barOuterPad = 0
var barPad = 0
var xScale = d3.scale.ordinal()
    .rangeRoundBands([0, 600],.1);
var xAxis = d3.svg.axis()
    .scale(xScale)
    .orient("bottom")
	
var yAxis = d3.svg.axis()
    .scale(yScale)
    .orient("left")
	.tickFormat(d3.format(""));
			
d3.csv("finaloutput.csv", function(error, data) {		
data = data.map(function(d){
    //d.Subway = d.Subway,	
    d.Count = +d.Count; 
    return d;
});


yScale.domain([0, d3.max(data, function(d){ return d.Count; })]);


console.log(data.map(function(d){ return d.Subway; }));
xScale.domain(data.map(function(d){ return d.Subway; }));
svg.append("g")
    .attr("transform", "translate(" + margin.left + "," + margin.top + ")")
    .selectAll(".bar")
    .data(data)
    .enter()
    .append("rect")
    .attr("class", "bar")
    .attr("x", function(d){ return xScale(d.Subway); })
    .attr("y", function(d){ return yScale(d.Count); })
    .attr("height", function(d){ return height - margin.top - margin.bottom - yScale(d.Count); })
    .attr("width", "40px");

	svg.append("g")
    .attr("class", "y axis")
    .attr("transform", "translate(" + margin.left + "," + margin.top + ")")
    .call(yAxis);
	
svg.append("g")
    .attr("class", "x axis")
    .attr("transform", "translate(" + margin.left + "," + (height - margin.bottom) + ")")
    .call(xAxis)
	.selectAll("text")  
    .style("text-anchor", "end")
    .attr("dx", "-.8em")
    .attr("dy", ".15em")
     .attr("transform", "rotate(-65)" );;

svg.append("g")
    .attr("transform", "translate(" + margin.left + "," + margin.top + ")")
    .selectAll(".textlabel")
    .data(data)
    .enter()
    .append("text")
    .attr("class", "textlabel")
    .attr("x", function(d){ return xScale(d.Subway) + ((xScale.rangeBand()/2) - 20); })
    .attr("y", function(d){ return yScale(d.Count) - 3; })
    .text(function(d){ return d3.format()(d.Count); });
});
