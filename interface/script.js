// pie geometry
var w = 550;                        // width
var h = 300;                        // height
var r = 100;                        // radius
var ir = 45;                        // inner radius
var textOffset = 14;

// stack geometry
var wstack = 600;
var hstack = 300;
var hpath_stack = hstack - 30;

color = d3.scale.category20();      // color function

var files_xml = [];
var dataxml = [];
var data;
var index_data = 0;
var owners = [];
var sitename = "UNKNOWN";
var totalGb = "UNKOWN";

d3.text("xml_list", function(list) {
    files_xml = list.split("\n");
    // download xmls
    var remaining_xml = files_xml.length;
    files_xml.map(function(d,i) {
	d3.xml(d, function(xml) {
	    dataxml[i] = load_xml(xml);
	    dataxml[i].map(function(d) { if (owners.indexOf(d.owner)<0) owners.push(d.owner); });
	    if (!--remaining_xml) {
		add_missing_owners(dataxml, owners);
		d3.select("#sitename").text(get_sitename(dataxml) + " usage");
		for (var j=0; j<dataxml.length; j++) {
		    dataxml[j].sort(function(a,b) { return d3.ascending(a.owner, b.owner); });
		}
		update(dataxml.length - 1); draw_stack();
	    }
	});
    });
} );


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

function get_sitename(data)
{
    sitename = data[0].sitename;
    for (var i=0; i<data.length; ++i)
    {
	if (data[i].sitename != sitename) sitename = "ERROR on SITENAME";
    }
    return sitename
}

function get_summary_user(user_data)
{
    return nameFromOwner(user_data.owner) + '\n' + (user_data.size / 1024/1024).toFixed(2) + ' Tb' + " (" + (user_data.size/ 1024. / totalGb * 100.).toFixed(1) + "%)";
}

function update(i)
{
    index_data = i;
    data = dataxml[i];
    totalGb = d3.sum(data.map(function(x) {return x.size})) / 1024.;
    draw_pie();
}

// load xml
function load_xml(xml)
{
    var result = [];
    var xmlroot = xml.documentElement;
    var metadata = xmlroot.getElementsByTagName("metadata");
    var sitename = metadata[0].getElementsByTagName("sitename")[0].childNodes[0].nodeValue;
    var time = metadata[0].childNodes[0].childNodes[0].nodeValue
    var time = new Date(time);
    var data = xmlroot.getElementsByTagName("data")[0];
    var owners_xml = data.getElementsByTagName("owner");
    for (var i=0; i<owners_xml.length; ++i)
    {
	var userdata = owners_xml[i];
	result.push({"owner": userdata.getAttribute("name"),
		     "files": userdata.getAttribute("files"),
		     "size": userdata.getAttribute("size")});
    }
    result.sitename = sitename;
    result.time = time;
    return result;
}




function nameFromOwner(owner) { return owner.substr(owner.lastIndexOf("/CN=") + 4); }

function highlight(i)
{
    d3.selectAll("#path_stack" + i).attr("style", "fill: #ff0");
    d3.selectAll("#path_pie" + i).attr("style", "fill: #ff0");
}

function dehighlight(i)
{
    d3.selectAll("#path_stack" + i).attr("style", "fill: " + color(i));
    d3.selectAll("#path_pie" + i).attr("style", "fill: " + color(i));
}

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
	.on("mouseover", function(d,i) { highlight(i); })
	.on("mouseout", function(d,i) { dehighlight(i) })
    
    var cells = rows.selectAll("td")
	.data(function(row, i) {
	    return columns.map(function(column) {
		if (column == "owner")
                    return {column: column, value: '<a href=\"' + sitename + '_filelist_user' + i + '.html\">' + row[column] + '</a>'};
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
    var n = d3.max(dataxml.map(function(d) {return d.length})); // number of layers
    var m = dataxml.length; // number of samples per layer

    myinput = d3.transpose(dataxml).map(function(d,i) {return d.map(function(dd,ii) {return {x:dataxml[ii].time.getTime(), y:parseInt(dd.size)};})});
    data_stack = d3.layout.stack().order("inside-out").offset("zero")(myinput);
    color_stack = d3.interpolateRgb("#aad", "#556");

    var w = wstack,
    h = hstack,
    hpath = hpath_stack,
    //    mx = m - 1,
    my = d3.max(data_stack, function(d) {
	return d3.max(d, function(d) {
	    return d.y0 + d.y;
	});
    });

    var min_timestamp = d3.min(dataxml.map(function(d,i) { return d.time.getTime(); }));
    var max_timestamp = d3.max(dataxml.map(function(d,i) { return d.time.getTime(); }));

    var mx = max_timestamp - min_timestamp;

    var area = d3.svg.area()
	.x(function(d) { return (d.x - min_timestamp) / mx * w; })
	.y0(function(d) { return hpath - d.y0 * hpath / my; })
	.y1(function(d) { return hpath - (d.y + d.y0) * hpath / my; });

    var vis_stack = d3.select("#stackchart")
	.append("svg")
	.attr("width", w)
	.attr("height", h).style("padding", 10);    

    paths = vis_stack.append("svg:g").attr("class", "paths").attr("transform", "translate(0," + 0 + ")");

    paths.selectAll("path")
	.data(data_stack)
	.enter().append("path")
	.style("fill", function(d,i) { return color(i); })
	.attr("id", function(d,i) { return "path_stack" + i;})
	.on("mouseover", function(d, i){ highlight(i); })
	.on("mouseout", function(d,i){ dehighlight(i); })
	.attr("d", area)
	.append("svg:title").text(function(d,i) { return get_summary_user(dataxml[dataxml.length - 1][i]); });
    
    rules = vis_stack.append("svg:g")
	.attr("class", "rules");

    // y-axis line
/*    rules.append("line")
	.attr("x1", 0)
	.attr("y1", 0)
	.attr("y2", hpath)
	.attr("stroke-width", 1)
	.style("stroke", "#000");*/
    
    // y ticks
    totalGb_round = (totalGb / 100.).toFixed() * 100;
    console.log(totalGb);
    console.log(totalGb_round);
    var y = d3.scale.linear().domain([0, totalGb_round]).range([0, hpath]);
    vis_stack.append("svg:g").attr("class", "labels").selectAll("yticks").data(d3.range(0,totalGb_round, totalGb_round/10)).enter().append("line")
	.attr("y1", function(d,i) { return y(d); })
	.attr("y2", function(d,i) { return y(d); })
	.attr("x1", 0)
	.attr("x2", w)
	.attr("stroke-width", 0.2)
	.style("stroke", "#222")
  
    // y labels
    vis_stack.append("svg:g").attr("class", "labels").selectAll("ylabels").data(d3.range(0,totalGb_round, totalGb_round/5)).enter().append("text")
	.attr("x", w)
	.attr("y", function(d,i) { return y(d); })
	.attr("dx", -5)
	.attr("dy", -2)
	.attr("text-anchor", "end")
	.style("font", "9px sans-serif")
	.style("fill", "#555")
	.text(function(d) { return totalGb_round - d + " Gb"; });
										
										

    // x-axis line
    rules.append("line")
	.attr("x1", 0)
	.attr("x2", w)
	.attr("y1", hpath)
	.attr("y2", hpath)
	.attr("stroke-width", 1)
	.style("stroke", "#000");

    // x-axis labels
    var x = d3.scale.linear().domain([min_timestamp, max_timestamp]).range([0, w]);
    vis_stack.append("svg:g").attr("class", "labels").selectAll("text").data(d3.range(0,1+0.2,0.2)).enter().append("text")
	.attr("text-anchor", "middle")
	.style("font", "10px sans-serif")
	.attr("transform", function(d,i) {return "translate(" + d*w + "," + h +")"; })    
	.text(function(d,i) { return d3.time.format("%d/%m/%Y")(new Date(x.invert(d*w)));});

    // x ticks
    vis_stack.append("svg:g").attr("class", "labels").selectAll("xticks").data(d3.range(0,1+0.2,0.2)).enter().append("line")
	.attr("x1", function(d,i) { return d*w; })
	.attr("x2", function(d,i) { return d*w; })
	.attr("y1", hpath-hpath/25).attr("y2", hpath)
	.attr("stroke-width", 1)
	.style("stroke", "#222")
    
    
    // marker triangles
    vis_stack.append("svg:g").attr("class", "triangles").selectAll("triangles").data(dataxml).enter().append("path")
	.attr("d", "M -5 10 L5 10 L0 0 Z")
	.attr("transform", function(d) {
	    return "translate(" + x(d.time.getTime()) + ", " + (hpath+3)  + ")"; })
	.style("stroke", "#000")
	.style("fill", function(d,i) {if (i!=index_data) return "#fff";})
	.on("click", function(d,i){ d3.selectAll(".triangles").selectAll("*").call(function(d) {d.style("fill", "#fff");}); update(i); d3.select(this).style("fill", "#000"); })
	.on("mouseover", function(d,i) { if (i!=index_data) d3.select(this).style("fill", "#555"); })
	.on("mouseout", function(d,i) { if (i!=index_data) d3.select(this).style("fill", "#fff"); })
	.attr("id", function(d,i) { return "triangle_marker_" + i; });
    
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

function draw_pie() {
    d3.select("#summarytable").html("");
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
	.on("mouseover", function(d,i){ highlight(i); })
	.on("mouseout", function(d,i){ dehighlight(i); })
	.append("svg:title").text(function(d,i) { return get_summary_user(d.data); });

// update tiles (not enter selection)
//    paths.attr("svg:title").text(function(d,i) {console.log("X"); return get_summary_user(d.data); });

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

