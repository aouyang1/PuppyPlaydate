$(function () {

    $(document).ready(function() {


        var countiesMap = Highcharts.geojson(Highcharts.maps['countries/us/us-all-all']);
        var lines = Highcharts.geojson(Highcharts.maps['countries/us/us-all-all'], 'mapline');

  	    var monthly_meetups = new Highcharts.Chart({
              chart: {renderTo: 'monthly_meetups',
                      type: 'line',
                      zoomType: 'x',
                      resetZoomButton: { position: { x: 0,
                                                     y: -30
                                                   }
                                       }
                      },
              title: {"text": "Meetups in " + county + ", " + state + " each month"},
              xAxis: {"type": 'datetime', "title": {"text": 'Date'}},
              yAxis: {"title": {"text": '# of Meetups'}, "min": 0},
              series: [{"name": county + ", " + state, "data": historical_data}],
              exporting: {
                 buttons: {
                    contextButton: {
                        menuItems: [{text: 'Monthly',
                                     onclick: function() {
                                        var chart_series = this.series[0]
                                        console.log("Monthly chart")
                                        console.log(chart_series)
                                        $.ajax({ type: "GET",
                                                 url: 'update_chart/month/us-tx-121/',
                                                 success: function(data_sel) {
                                                    console.log("plotting monthly")
                                                    chart_series.setData(data_sel.historical_data)
                                                 }
                                               })
                                        }
                                     },
                                     {text: 'Daily',
                                     onclick: function() {
                                        var chart_series = this.series[0]
                                        console.log("Daily chart")
                                        $.ajax({ type: "GET",
                                                 url: 'update_chart/month/us-tx-121/',
                                                 success: function(data_sel) {
                                                    console.log("plotting daily")
                                                    chart_series.setData(data_sel.historical_data)
                                                 }
                                               })
                                        }
                                     },
                                     {text: 'Hourly',
                                     onclick: function() {
                                        var chart_series = this.series[0]
                                        console.log("Hourly chart")
                                        $.ajax({ type: "GET",
                                                 url: 'update_chart/month/us-tx-121/',
                                                 success: function(data_sel) {
                                                    console.log("plotting hourly")
                                                    chart_series.setData(data_sel.historical_data)
                                                 }
                                               })
                                        }
                                     }]
                        }
                    }
                 }

  	    });

        var county_map = new Highcharts.Map({
            chart: {renderTo: 'county_map',

                    events: { load: function () {
                                var seriesData = this.series[0];

                                setInterval(function () {
                                    $.ajax({ type: "GET",
                                         url: '/update_map/',
                                         success: function(data) {
                                            seriesData.setData(data.rt_data);
                                         }
                                    })
                                }, 5000);


                            }
                        }},
            title : { text : 'Current Messages by County' },
            legend: { title: { text: '# of Messages',
                               style: { color: (Highcharts.theme && Highcharts.theme.textColor) || 'black' } },
                               layout: 'vertical',
                               align: 'right',
                               floating: true,
                               valueDecimals: 0,
                               backgroundColor: (Highcharts.theme && Highcharts.theme.legendBackgroundColor) || 'rgba(255, 255, 255, 0.85)',
                               symbolRadius: 0,
                               symbolHeight: 14 },
            mapNavigation: { enabled: true,
                             enableMouseWheelZoom: false},
            colorAxis: { dataClasses: [{ from: 0, to: 2, color: "#ffffcc" },
                                       { from: 2, to: 4, color: "#ffeda0" },
                                       { from: 4, to: 6, color: "#fed976" },
                                       { from: 6, to: 8, color: "#feb24c" },
                                       { from: 8, to: 10, color: "#fd8d3c" },
                                       { from: 10, to: 12, color: "#fc4e2a" },
                                       { from: 12, to: 14, color: "#e31a1c" },
                                       { from: 14, to: 16, color: "#bd0026" },
                                       { from: 16, color: "#800026" }] },
            plotOptions: { mapline: { showInLegend: false, enableMouseTracking: false },
                           series: { cursor: 'pointer',
                                     allowPointSelect: true,
                                     point: {
                                        events: {
                                            click: function (event) {
                                                var county_code = event.currentTarget.code
                                                $.ajax({ type: "GET",
                                                         url: '/new_messages/' + county_code + "/",
                                                         success: function(data) {
                                                            msg_len = data.msg.length
                                                            document.getElementById("news_feed_title").innerHTML = "Current Meetups in " + data.county + ", " + data.state;
                                                            for (i = 0; i < 10; i++) {
                                                                if (i < msg_len) {
                                                                    document.getElementById("message" + (i+1).toString()).innerHTML = data.msg[i];
                                                                }else{
                                                                    document.getElementById("message" + (i+1).toString()).innerHTML = "";
                                                                }
                                                            }
                                                         }
                                                })
                                                $.ajax({ type: "GET",
                                                         url: '/update_chart/month/' + county_code + "/",
                                                         success: function(data) {
                                                            var monthly_meetups = new Highcharts.Chart({
                                                                chart: {renderTo: 'monthly_meetups', type: 'line'},
                                                                title: {"text": "Meetups in " + data.county + ", " + data.state + " each month"},
                                                                xAxis: {"type": 'datetime', "title": {"text": 'Date'}},
                                                                yAxis: {"title": {"text": '# of Meetups'}, "min": 0},
                                                                series: [{"name": data.county + ", " + data.state, "data": data.historical_data}],
                                                                exporting: {
                                                                 buttons: {
                                                                    contextButton: {
                                                                        menuItems: [{text: 'Monthly',
                                                                                     onclick: function() {
                                                                                        var chart_series = this.series[0]
                                                                                        console.log("Monthly chart")
                                                                                        console.log(chart_series)
                                                                                        $.ajax({ type: "GET",
                                                                                                 url: 'update_chart/month/' + county_code + "/",
                                                                                                 success: function(data_sel) {
                                                                                                    console.log("plotting monthly")
                                                                                                    chart_series.setData(data_sel.historical_data)
                                                                                                 }
                                                                                               })
                                                                                        }
                                                                                     },
                                                                                     {text: 'Daily',
                                                                                     onclick: function() {
                                                                                        var chart_series = this.series[0]
                                                                                        console.log("Daily chart")
                                                                                        $.ajax({ type: "GET",
                                                                                                 url: 'update_chart/day/' + county_code + "/",
                                                                                                 success: function(data_sel) {
                                                                                                    console.log("plotting daily")
                                                                                                    chart_series.setData(data_sel.historical_data)
                                                                                                 }
                                                                                               })
                                                                                        }
                                                                                     },
                                                                                     {text: 'Hourly',
                                                                                     onclick: function() {
                                                                                        var chart_series = this.series[0]
                                                                                        console.log("Hourly chart")
                                                                                        $.ajax({ type: "GET",
                                                                                                 url: 'update_chart/hour/' + county_code + "/",
                                                                                                 success: function(data_sel) {
                                                                                                    console.log("plotting hourly")
                                                                                                    chart_series.setData(data_sel.historical_data)
                                                                                                 }
                                                                                               })
                                                                                        }
                                                                                     }]
                                                                        }
                                                                    }
                                                                 }

                                                            });

                                                         }
                                                })

                                            }
                                        }
                                     },

                                     marker: { lineWidth: 1 }

                                    }
                          },
            series : [{mapData : countiesMap,
                       data: null,
                       joinBy: ['hc-key', 'code'],
                       name: '# of Messages',
                       borderWidth: 0.5,
                       states: { hover: { color: '#bada55' } } },
                      {type: 'mapline',
                       name: 'State borders',
                       data: [lines[0]],
                       color: 'black' },
                      {type: 'mapline',
                       name: 'Separator',
                       data: [lines[1]],
                       color: 'gray' }]
	    });

	});
});