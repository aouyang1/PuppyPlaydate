$(function () {

    $(document).ready(function() {
        println(county)
	    var monthly_meetups = new Highcharts.Chart({
            chart: {renderTo: 'monthly_meetups', type: 'line', height: 300},
            title: {"text": "Meetups in " + county + ", " + state + " each month"},
            xAxis: {"type": 'datetime', "title": {"text": 'Date'}},
            yAxis: {"title": {"text": '# of Meetups'}, "min": 0},
            series: [{"name": county + ", " + state, "data": historical_data}]
	    });

        var county_map = new Highcharts.Chart({
            chart: {renderTo: 'county_map', type: 'line', height: 300},
            title: {"text": "Meetups in " + county + ", " + state + " each month"},
            xAxis: {"type": 'datetime', "title": {"text": 'Date'}},
            yAxis: {"title": {"text": '# of Meetups'}, "min": 0},
            series: [{"name": county + ", " + state, "data": historical_data}]
	    });

	});
});