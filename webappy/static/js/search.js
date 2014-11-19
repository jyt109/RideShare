        //search box


$.fn.dataTable.ext.search.push(
        function( settings, data, dataIndex ) {
            console.log('called...');

            var min = parseInt( $('#min').val(), 10 );
            var max = parseInt( $('#max').val(), 10 );
            var score = parseFloat( data[5] ) || 0; // use data for the age column
            if (nanOrCompare(min, max, score))
            {
                return true;
            }
            return false;
});
    

function nanOrCompare(min, max, field) {
            return       (isNaN( min ) && isNaN( max ) ) ||
                         ( isNaN( min ) && field <= max ) ||
                         ( min <= field   && isNaN( max ) ) ||
                         ( min <= field   && field <= max );
}
