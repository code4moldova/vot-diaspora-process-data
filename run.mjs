import fs from 'fs'
import stringify from 'csv-stringify/lib/sync.js';

// https://services8.arcgis.com/rPU79ZA4dDTs1VpS/ArcGIS/rest/services/Prodaction/FeatureServer/0/query?where=BESV_diaspora+%3C%3E+%27%27&objectIds=&time=&geometry=&geometryType=esriGeometryPoint&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=OBJECTID_1%2CRaion%2CComuna%2CGlobalID%2CLocalul%2CAdresa%2CPoolingstationID%2Cname%2CnameRu%2Cname1%2CnameRu1%2CgeoLatitude%2CgeoLongitude%2CBESV%2CSectia_de_vot%2CGlobalID_1%2CCircumscriptia_electorala_1%2CTelefon_1%2Clincul%2CTara%2CLocalitatea_diaspora%2Clink%2CLink_1%2CCircum_sectia%2COBJECTID%2CBESV_diaspora&returnGeometry=true&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=html&token=
const data = JSON.parse(fs.readFileSync('./input.geojson'))

const sectiiFinale = data.features
    .sort((a, b) => a.properties.Sectia_de_vot - b.properties.Sectia_de_vot)
    .map((feature, index, array) => ({
        ...feature,
        properties: {
            ...feature.properties,
            // In case country is missing, and between 2 identical, use them (make sure array is sorted, by station number)
            Tara: !feature.properties.Tara && array[index - 1]?.properties.Tara === array[index + 1]?.properties.Tara
                ? array[index - 1].properties.Tara
                : feature.properties.Tara
        }
    }))
    .map((feature, index) => {
        const [Longitude, Latitude] = feature.geometry.coordinates
        const { Tara, Adresa, Sectia_de_vot, Localitatea_diaspora } = feature.properties

        // Replace multiple spaces or newlines with a single space, trim
        const adjust = (field) => (field ?? '').replace(/(\s+)/g, ' ').trim()

        return {
            Id: index + 1,
            PollingStationNumber: `38/${Sectia_de_vot}`,
            Latitude,
            Longitude,
            // Yes it is `County`, this is DB column name :)
            County: adjust(Tara),
            Address: adjust(Adresa),
            Locality: adjust(Localitatea_diaspora),
            Institution: adjust(Localitatea_diaspora),
        }
    })

const output = stringify(sectiiFinale, { header: true })
const outputGeoJson = {
    type: "FeatureCollection",
    features: sectiiFinale.map(sectie => ({
        type: "Feature",
        id: sectie.PollingStationNumber,
        geometry: {
            type: "Point",
            coordinates: [sectie.Longitude, sectie.Latitude]
        },
        properties: sectie
    }))
}

fs.writeFileSync('./output.csv', output, { encoding: 'utf8' })
fs.writeFileSync('./output.geojson', JSON.stringify(outputGeoJson, null, 2), { encoding: 'utf8' })

