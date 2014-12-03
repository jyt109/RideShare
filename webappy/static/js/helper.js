function format (d) {
    var ride1P = d[6],
    ride2P = d[7];
    if (d[10] === 1) {
        var ride1D = d[8],
        ride2D = d[9];
    } else if(d[10] === 2){
        var ride1D = d[8],
        ride2D = d[9];
    } else {
        console.log('unknown ride type');
    }

    // `d` is the original data object for the row
    return '<div class="extraInfoDiv">' +
                '<div>' +
                    '<p class="rideInfoa zero">Ride A: &nbsp</p>' +
                    '<p class="rideInfoa one">' + ride1P + '<img src="../static/images/glyphicons_217_circle_arrow_right.png" class="right_arr"></p>' +
                    '<p class="rideInfoa two">' + ride1D + '</p>'+ '<div class="spacer"></div>' +
                '</div>' +

                '<div>' +
                    '<p class="rideInfob zero">Ride B: &nbsp</td>' +
                    '<p class="rideInfob one">' + ride2P + '<img src="../static/images/glyphicons_217_circle_arrow_right.png" class="right_arr"></p>' +
                    '<p class="rideInfob two">' + ride2D + '</p>'+
                '</div>' +
            '</div>' +
            '<button onClick="get_ride_share()" type="submit" name="submit" class="btn btn-danger route_btn" disabled>Ride Share</button>';
};

function parse_to_float(data) {
    var lst = [];
    for (var i=0; i < data.length; i++) {
        lst.push(data[i].map(parseFloat));
    }
    return lst
};

function click_to_remove() {
    map.removeLayer(cline);
    map.removeLayer(mline);
    map.removeLayer(start_blue_mark);
    map.removeLayer(end_blue_mark);
    map.removeLayer(start_green_mark);
    map.removeLayer(end_green_mark);
    $('.leaflet-zoom-hide').remove()
};

function hideLoading() {
    $('#loading').removeClass('work');
};
function showLoading() {
    $('#loading').addClass('work');
};
function clearClickedOnPage() {
    var table1 = $('#rs_entries').DataTable();
    var tr = $('tr.shown'); //return dom
    var row1 = table1.row(tr); //return data
    row1.child.hide();
    $('tr.shown + tr').remove();
    tr.removeClass('shown');
    $('#rs_entries').find('tbody tr').css('background-color', 'white');
};
