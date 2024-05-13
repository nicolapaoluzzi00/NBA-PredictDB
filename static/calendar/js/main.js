(function($) {

	"use strict";

	// Setup the calendar with the current date
$(document).ready(function(){
    // Inserisco tutte le partite tra gli eventi
    const games = document.getElementById('a').getAttribute('games');
    var json_games = JSON.parse(games)
    read_events(json_games)

    // Creo calendario
    var date = new Date();
    var today = date.getDate();
    // Set click handlers for DOM elements
    $(".right-button").click({date: date}, next_year);
    $(".left-button").click({date: date}, prev_year);
    $(".month").click({date: date}, month_click);
    //$("#add-button").click({date: date}, new_event);
    // Set current month as active
    $(".months-row").children().eq(date.getMonth()).addClass("active-month");
    init_calendar(date);
    var events = check_events(today, date.getMonth()+1, date.getFullYear());
    show_events(events, months[date.getMonth()], today);
});

function read_events(json_games){
    for (const game in json_games) {
        //console.log(json_games[game]['start_time']);
        var date = json_games[game]['start_time'].split(' ')[0]
        new_event_json(json_games[game]['game_id'], json_games[game]['home_team_name'], json_games[game]['away_team_name'],
                        parseInt(date.split('-')[0]), parseInt(date.split('-')[1]), parseInt(date.split('-')[2]),
                        json_games[game]['home_pts'], json_games[game]['away_pts'])
    }
}

// Initialize the calendar by appending the HTML dates
function init_calendar(date) {
    $(".tbody").empty();
    $(".events-container").empty();
    var calendar_days = $(".tbody");
    var month = date.getMonth();
    var year = date.getFullYear();
    var day_count = days_in_month(month, year);
    var row = $("<tr class='table-row'></tr>");
    var today = date.getDate();
    // Set date to 1 to find the first day of the month
    date.setDate(1);
    var first_day = date.getDay();
    // 35+firstDay is the number of date elements to be added to the dates table
    // 35 is from (7 days in a week) * (up to 5 rows of dates in a month)
    for(var i=0; i<35+first_day; i++) {
        // Since some of the elements will be blank, 
        // need to calculate actual date from index
        var day = i-first_day+1;
        // If it is a sunday, make a new row
        if(i%7===0) {
            calendar_days.append(row);
            row = $("<tr class='table-row'></tr>");
        }
        // if current index isn't a day in this month, make it blank
        if(i < first_day || day > day_count) {
            var curr_date = $("<td class='table-date nil'>"+"</td>");
            row.append(curr_date);
        }   
        else {
            var curr_date = $("<td class='table-date'>"+day+"</td>");
            var events = check_events(day, month+1, year);
            if(today===day && $(".active-date").length===0) {
                curr_date.addClass("active-date");
                show_events(events, months[month], day);
            }
            // If this date has any events, style it with .event-date
            if(events.length!==0) {
                curr_date.addClass("event-date");
            }
            // Set onClick handler for clicking a date
            curr_date.click({events: events, month: months[month], day:day}, date_click);
            row.append(curr_date);
        }
    }
    // Append the last row and set the current year
    calendar_days.append(row);
    $(".year").text(year);
}

// Get the number of days in a given month/year
function days_in_month(month, year) {
    var monthStart = new Date(year, month, 1);
    var monthEnd = new Date(year, month + 1, 1);
    return (monthEnd - monthStart) / (1000 * 60 * 60 * 24);    
}

// Event handler for when a date is clicked
function date_click(event) {
    $(".events-container").show(250);
    $("#dialog").hide(250);
    $(".active-date").removeClass("active-date");
    $(this).addClass("active-date");
    show_events(event.data.events, event.data.month, event.data.day);
};

// Event handler for when a month is clicked
function month_click(event) {
    $(".events-container").show(250);
    $("#dialog").hide(250);
    var date = event.data.date;
    $(".active-month").removeClass("active-month");
    $(this).addClass("active-month");
    var new_month = $(".month").index(this);
    date.setMonth(new_month);
    init_calendar(date);
}

// Event handler for when the year right-button is clicked
function next_year(event) {
    $("#dialog").hide(250);
    var date = event.data.date;
    var new_year = date.getFullYear()+1;
    $("year").html(new_year);
    date.setFullYear(new_year);
    init_calendar(date);
}

// Event handler for when the year left-button is clicked
function prev_year(event) {
    $("#dialog").hide(250);
    var date = event.data.date;
    var new_year = date.getFullYear()-1;
    $("year").html(new_year);
    date.setFullYear(new_year);
    init_calendar(date);
}

// Adds a json event to event_data
function new_event_json(game_id, home, away, year, month, day, home_pts, away_pts) {
    var event = {
        "game_id": game_id,
        "home_team": home,
        "away_team": away,
        "year": year,
        "month": month,
        "day": day,
        "home_pts": home_pts,
        "away_pts": away_pts
    };
    event_data["events"].push(event);
}

// Display all events of the selected date in card views
function show_events(events, month, day) {
    // Clear the dates container
    $(".events-container").empty();
    $(".events-container").show(250);
    //console.log(event_data["events"]);
    // If there are no events for this date, notify the user
    if(events.length===0) {
        var event_card = $("<div class='event-card'></div>");
        //var event_name = $("<div class='event-name'>Non ci sono partite pianificate per il giorno "+day+" "+month+".</div>");
        var event_name = $("<div class='event-name'>Non ci sono partite pianificate.</div>");
        $(event_card).css({ "border-left": "10px solid #FF1744" });
        $(event_card).append(event_name);
        $(".events-container").append(event_card);
    }
    else {
        // Go through and add each event as a card to the events container
        for(var i=0; i<events.length; i++) {
            var event_card = $("<a class='event-card' href='/game_details?game_id="+events[i]['game_id']+"' color = black></a>");
            if (events[i]["home_pts"] == null){
                var event_name = $("<div class='event-name'>"+events[i]["home_team"]+" - "+events[i]["away_team"]+"</div>");
            }else{
                var event_name = $("<div class='event-name'>"+events[i]["home_team"]+" ("+events[i]["home_pts"]+") - ("+events[i]["away_pts"]+") "+events[i]["away_team"]+"</div>");
            }
            $(event_card).append(event_name);
            $(".events-container").append(event_card);
        }
    }
}

// Checks if a specific date has any events
function check_events(day, month, year) {
    var events = [];
    for(var i=0; i<event_data["events"].length; i++) {
        var event = event_data["events"][i];
        if(event["day"]===day &&
            event["month"]===month &&
            event["year"]===year) {
                events.push(event);
            }
    }
    return events;
}

// Given data for events in JSON format
var event_data = {
    "events": [
    ]
};

const months = [ 
    "Gennaio", 
    "Febbraio", 
    "Marzo", 
    "Aprile", 
    "Maggio", 
    "Giugno", 
    "Luglio", 
    "Agosto", 
    "Settembre", 
    "Ottobre", 
    "Novembre", 
    "Dicembre" 
];

})(jQuery);
