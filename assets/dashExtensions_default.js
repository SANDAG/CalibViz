window.dashExtensions = Object.assign({}, window.dashExtensions, {
    default: {
        function0: function(feature) {
                return {
                    color: 'black',
                    weight: 1,
                    fillColor: feature.properties.color,
                    fillOpacity: 0.7
                }
            }

            ,
        function1: function(feature) {
            return {
                weight: 3,
                color: '#333',
                fillOpacity: 0.9
            };
        }

    }
});