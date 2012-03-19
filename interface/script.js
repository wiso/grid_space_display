// pie size

var w = 550;                        // width
var h = 300;                        // height
var r = 100;                        // radius
var ir = 45;
var textOffset = 14;

color = d3.scale.category20();      // color function

var files = ["old_total.csv", "data_total.csv", "data3.csv"];
var datas = [];
var data;
var owners = [];

// with this function every data from every files has the same owners (if one is missing fill with null owner)
function add_missing_owners(d, o)
{
    o.map(function(owner) // TODO: find better way
	  {
	      d.map(function(row) {
		  var ii = 0;
		  var iiend = row.length;
		  var found = 0;
		  for (ii = 0; ii < iiend; ii++)
		  {
		      if (row[ii].owner == owner) found = 1;
		  }
		  if (found == 0)
		  {
		      row.push({"owner": owner, files: 0, size:0});
		  }
	      });
	  });
}

// load the data
var remaining = files.length;
files.map(function(d,i) {
    d3.csv(d, function(csv) {
	datas[i] = csv;
	console.log(datas[i]);
	datas[i].map(function(d) { if (owners.indexOf(d.owner)<0) owners.push(d.owner); });
	if (!--remaining) { 
	    add_missing_owners(datas, owners);
	    var j=0;
	    for (j=0; j<datas.length; j++) {
		datas[j].sort(function(a,b) { return d3.ascending(a.owner, b.owner); });
	    }
	    data = datas[datas.length - 1]; update(); draw_stack(); 
	};
    })
});

// add the button at the top
d3.select("#buttons").selectAll("div").data(files).enter().append("button").attr("type", "button").text(function(d) { return d; }).on("click", function(d,i) {data = datas[i]; update()});


function nameFromOwner(owner) { return owner.substr(owner.lastIndexOf("/CN=") + 4); }

function tabulate(data, columns) {
    var table = d3.select("#summarytable").append("table").attr("class","hor-minimalist-b");
    var thead = table.append("thead");
    var tbody = table.append("tbody");

    thead.append("tr")
	.selectAll("th")
	.data(columns)
	.enter()
	.append("th")
	.text(function(column) { return column; });

    var rows = tbody.selectAll("tr")
	.data(data)
	.enter()
	.append("tr")
	.on("mouseover", function(d,i) {
	    d3.selectAll("#path_stack" + i).attr("style", "fill: #ff0");
	    d3.selectAll("#path_pie" + i).attr("style", "fill: #ff0")})
	.on("mouseout", function(d,i) {
	    d3.selectAll("#path_stack" + i).attr("style", "fill: " + color(i));
	    d3.selectAll("#path_pie" + i).attr("style", "fill: " + color(i))});
    
    var cells = rows.selectAll("td")
	.data(function(row, i) {
	    return columns.map(function(column) {
		if (column == "owner")
		    return {column: column, value: '<a href=\"datasetlist_' + i + '.html\">' + row[column] + '</a>'};
		else return {column: column, value: row[column]};
	    });
	})
	.enter()
	.append("td")
	.html(function(d) { return d.value; });

    function donut2(i)
    {
	function donut(d)
	{
	    return {x:i, y:d.size};
	}
	return donut;
    }
    
    var h_user = 30;
    var hpath_user = h_user - 3;
    var w_user = 100;

    var vis_user = rows.append("td").append("svg:svg").attr("width", w_user).attr("height", h_user);
    vis_user.append("svg:g").attr("class", "rules").append("line").attr("x1", 0).attr("x2", w_user).attr("y1", h_user).attr("y2", h_user).attr("stroke-width", 1).style("stroke", "#000");


    function get_data_stack_user(i)
    {
	var myinput_user = [[(datas[0].map(donut2(0))[i]), (datas[1].map(donut2(1))[i])]];
	var data_stack_user = d3.layout.stack().order("inside-out").offset("zero")(myinput_user);
	return data_stack_user;
    }

    function get_area_user(i)
    {
	var m_user = 2;
	var n_user = 1;
	var mx_user = m_user - 1;
	var my_user = d3.max(get_data_stack_user(i), function(d) {
	    return d3.max(d, function(d) {
		return d.y0 + d.y;
	    });
	});

	var area_user = d3.svg.area()
	    .x(function(d) { return d.x * w_user / mx_user; })
	    .y0(function(d) { return hpath_user - d.y0 * hpath_user / my_user; })
	    .y1(function(d) { return hpath_user - (d.y + d.y0) * hpath_user / my_user; });
	return area_user;
    }

    //    paths = vis_user.append("svg:g").attr("class", "paths").selectAll("path").data(function(d,i) {return get_data_stack_user(i);}).enter()
    //	.append("path").style("fill", "#ccc").attr("d", function(d,i){return eval(get_area_user(i));});


    table.selectAll("thead th")
	.text(function(column) {
	    return column.charAt(0).toUpperCase() + column.substr(1);
	})
    thead.selectAll("tr").append("th").text("");
    
    table.selectAll("tbody tr")
	.sort(function(a,b) {
	    return d3.descending(parseInt(a.size), parseInt(b.size));
	});
    
    
    return table;
}


var arc = d3.svg.arc()
    .startAngle(function(d) { return d.startAngle; })
    .endAngle(function(d) { return d.endAngle; })
    .outerRadius(r)
    .innerRadius(ir);

var vis = d3.select("#piechart")
    .append("svg:svg")
    .attr("width", w)
    .attr("height", h);

var arc_group = vis.append("svg:g")
    .attr("class", "arc")
    .attr("transform", "translate(" + (w/2) + "," + (h/2) + ")");

var label_group = vis.append("svg:g")
    .attr("class", "label_group")
    .attr("transform", "translate(" + (w/2) + "," + (h/2) + ")");

var center_group = vis.append("svg:g")
    .attr("class", "center_group")
    .attr("transform", "translate(" + (w/2) + "," + (h/2) + ")");

//WHITE CIRCLE BEHIND LABELS
var whiteCircle = center_group.append("svg:circle")
    .attr("fill", "white")
    .attr("r", ir);

// "TOTAL" LABEL
var totalLabel = center_group.append("svg:text")
    .attr("class", "label")
    .attr("dy", -15)
    .attr("text-anchor", "middle") // text-align: right
    .text("TOTAL");






function draw_stack()
{
    var n = d3.max(datas.map(function(d) {return d.length})); // number of layers
    var m = datas.length; // number of samples per layer

    myinput = d3.transpose(datas).map(function(d,i) {return d.map(function(dd,ii) {return {x:ii, y:parseInt(dd.size)};})});
    data_stack = d3.layout.stack().order("inside-out").offset("zero")(myinput);
    color_stack = d3.interpolateRgb("#aad", "#556");


    var w = 600,
    h = 300,
    hpath = 270,
    mx = m - 1,
    my = d3.max(data_stack, function(d) {
	return d3.max(d, function(d) {
	    return d.y0 + d.y;
	});
    });

    var area = d3.svg.area()
	.x(function(d) { return d.x * w / mx; })
	.y0(function(d) { return hpath - d.y0 * hpath / my; })
	.y1(function(d) { return hpath - (d.y + d.y0) * hpath / my; });

    var vis_stack = d3.select("#stackchart")
	.append("svg")
	.attr("width", w)
	.attr("height", h).style("padding", 5);    

    paths = vis_stack.append("svg:g").attr("class", "paths").attr("transform", "translate(0," + 0 + ")");

    paths.selectAll("path")
	.data(data_stack)
	.enter().append("path")
	.style("fill", function(d,i) { return color(i); })
	.attr("id", function(d,i) { return "path_stack" + i;})
	.on("mouseover", function(d, i){
	    d3.select(this).attr("style", "fill: #ff0");
	    d3.select("#path_pie" + i).attr("style", "fill: #ff0");
	})
	.on("mouseout", function(d,i){
	    d3.select(this).attr("style", "fill:" + color(i));
	    d3.select("#path_pie" + i).attr("style", "fill:" + color(i));
	})
	.attr("d", area)
	.append("svg:title").text(function(d,i) {return nameFromOwner(datas[0][i].owner)});
    
    rules = vis_stack.append("svg:g")
	.attr("class", "rules");

    rules.append("line")
	.attr("x1", 0)
	.attr("x2", w)
	.attr("y1", hpath)
	.attr("y2", hpath)
	.attr("stroke-width", 1)
	.style("stroke", "#000");

    x = d3.scale.linear().domain([0, datas.length-1]).range([0, w]),
    vis_stack.append("svg:g").attr("class", "labels").selectAll("text").data(datas).enter().append("text")
	.attr("text-anchor", "middle")
	.attr("transform", function(d,i) {return "translate(" + x(i) + "," + h +")"; })    
	.text(function(d,i) { return i; });

}

//TOTAL VALUE
var totalValue = center_group.append("svg:text")
    .attr("class", "total")
    .attr("dy", 7)
    .attr("text-anchor", "middle") // text-align: right
    .text("");

//UNITS LABEL
var totalUnits = center_group.append("svg:text")
    .attr("class", "units")
    .attr("dy", 21)
    .attr("text-anchor", "middle") // text-align: right
    .text("Tb");

var donut = d3.layout.pie().value(function(d) { return d.size; });

var pieData = [];
var oldPieData = [];

function update() {
    d3.select("#summarytable").html("");
    var totalGb = d3.sum(data.map(function(x) {return x.size})) / 1024.;

    tableData = tabulate(data, ["owner", "files", "size"]);

    totalValue.text((totalGb / 1024.).toFixed(2));


    oldPieData = pieData;
    pieData = donut(data);
    
    var paths = arc_group.selectAll("path").data(pieData);
    paths.enter().append("svg:path")
	.attr("stroke", "white")
	.attr("stroke-width", 0.5)
	.attr("fill", function(d, i) { return color(i); })
	.attr("d", arc)
	.attr("id", function(d,i) {return "path_pie" + i;})
	.on("mouseover", function(d,i){
	    d3.selectAll("#path_stack" + i).attr("style", "fill: #ff0");
	    d3.select(this).attr("style", "fill: #ff0")})
	.on("mouseout", function(d,i){
	    d3.select(this).attr("style", "fill: " + color(i))
	    d3.select("#path_stack" + i).attr("style", "fill: " + color(i));
	});

    //DRAW TICK MARK LINES FOR LABELS
    lines = label_group.selectAll("line").data(pieData);
    lines.enter().append("svg:line")
	.attr("x1", 0)
	.attr("x2", 0)
	.attr("y1", -r+6)
	.attr("y2", -r-6)
	.attr("stroke", "gray")
	.attr("display", function(d) { return (d.value > totalGb * 1024 * 0.02) && (d.value < totalGb * 1024 * 0.08) ? null : "none"; })
	.attr("transform", function(d) {
	    return "rotate(" + (d.startAngle+d.endAngle)/2 * (180/Math.PI) + ")";
	});
    lines.transition()
	.duration(800)
	.delay(300)
	.attr("transform", function(d) {
	    return "rotate(" + (d.startAngle+d.endAngle)/2 * (180/Math.PI) + ")";
	});
    lines.exit().remove();

    // DRAW LABELS WITH NAMES
    var nameLabels = label_group.selectAll("text.units").data(pieData);

    nameLabels.enter().append("svg:text")
	.attr("class", "units")
	.attr("transform", function(d) {
	    return "translate(" + Math.cos(((d.startAngle+d.endAngle - Math.PI)/2)) * (r+textOffset) + "," + Math.sin((d.startAngle+d.endAngle - Math.PI)/2) * (r+textOffset) + ")";
	})
	.attr("dy", function(d){
	    if ((d.startAngle+d.endAngle)/2 > Math.PI/2 &&
		(d.startAngle+d.endAngle)/2 < Math.PI*1.5 ) {
		return 17;
	    } else {
		return 5;
	    }
	})
	.attr("text-anchor", function(d){
	    if ((d.startAngle+d.endAngle)/2 < Math.PI ) {
		return "beginning";
	    } else {
		return "end";
	    }
	})
	.attr("display", function(d) { return d.value > totalGb * 1024 * 0.02 ? null : "none"; })
	.text(function(d){
	    return nameFromOwner(d.data.owner);
	})
	.on("mouseover", function(){d3.select(this).attr("style", "font-size:20")})
	.on("mouseout", function(){d3.select(this).attr("style", "font-size:15")});
    
    nameLabels.transition().duration(800).delay(300).attrTween("transform", textTween);
    nameLabels.exit().remove();

    paths.transition().duration(500).ease("pol2").attrTween("d", arcTween);
    paths.exit().transition().duration(200).ease("pol2").attrTween("d", removePieTween).remove();
};

function arcTween(d, i) {
    var oldStart;
    var oldEnd;
    if(oldPieData[i]){
	oldStart = oldPieData[i].startAngle;
	oldEnd = oldPieData[i].endAngle;
    } else if (!(oldPieData[i]) && oldPieData[i-1]) {
	oldStart = (d.startAngle + d.endAngle) / 2.;
	oldEnd = (d.startAngle + d.endAngle) / 2.;
    } else if(!(oldPieData[i-1]) && oldPieData.length > 0){
	oldStart = oldPieData[oldPieData.length-1].endAngle;
	oldEnd = oldPieData[oldPieData.length-1].endAngle;
    } else {
	oldStart = 0;
	oldEnd = 0;
    }
    var i = d3.interpolate({startAngle: oldStart, endAngle: oldEnd}, {startAngle: d.startAngle, endAngle: d.endAngle});
    return function(t) {
	return arc(i(t));
    };
}

function removePieTween(d) {
    s0 = (d.startAngle + d.endAngle) / 2.;
    e0 = (d.startAngle + d.endAngle) / 2.;
    var i = d3.interpolate({startAngle: d.startAngle, endAngle: d.endAngle}, {startAngle: s0, endAngle: e0});
    return function(t) {
	return arc(i(t));
    };
}

function addPieTween(d) {
    s0 = (d.startAngle + d.endAngle) / 2.;
    e0 = (d.startAngle + d.endAngle) / 2.;
    var i = d3.interpolate({startAngle: s0, endAngle: e0}, {startAngle: d.startAngle, endAngle: d.endAngle});
    return function(t) {
	return arc(i(t));
    };
}

function textTween(d, i) {
    var a;
    if(oldPieData[i]){
	a = (oldPieData[i].startAngle + oldPieData[i].endAngle - Math.PI)/2;
    } else {
	a = (d.startAngle + d.endAngle - Math.PI)/2;
    }
    var b = (d.startAngle + d.endAngle - Math.PI)/2;
    
    var fn = d3.interpolateNumber(a, b);
    return function(t) {
	var val = fn(t);
	return "translate(" + Math.cos(val) * (r+textOffset) + "," + Math.sin(val) * (r+textOffset) + ")";
    };
}
