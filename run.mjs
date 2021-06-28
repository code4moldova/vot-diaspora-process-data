import fs from 'fs'
import https from 'https'
import parse from 'csv-parse/lib/sync.js'
import stringify from 'csv-stringify/lib/sync.js';

const originalLog = console.log
const logs = []
console.log = (...args) => {
    logs.push(args.map(a => JSON.stringify(a)).join(', '))
    originalLog(...args)
}

const source = fs.readFileSync('./input.csv')
const sectii = parse(source, {
    columns: true,
    trim: true,
    skip_empty_lines: true,
    delimiter: `;`,
    escape: `"`,
    quote: `"`
})

const procesareSectii = asyncPipe(
    remapareColoane,
    ajustareStatulPentruSectiiConsecutive,
    solicitaLocatiePentruAdrese,
)

const sectiiFinale = await procesareSectii(sectii)
const output = stringify(sectiiFinale, { header: true })
const geoJson = creazaGeoJson(sectiiFinale)

fs.writeFileSync('./output.csv', output, { encoding: 'utf8' })
fs.writeFileSync('./output.geojson', JSON.stringify(geoJson, null, 2), { encoding: 'utf8' })
fs.writeFileSync('./run.log', logs.join('\n'), { encoding: 'utf8' })

async function remapareColoane(sectii) {
    return sectii.map((sectie, index) => ({
        Id: index + 1,
        PollingStationNumber: sectie['Nr. SV'].trim(),
        Latitude: '',
        Longitude: '',
        // Yes it is `County` there, this is DB column name :)
        County: sectie['Misiunea / Statul'].trim(),
        Address: sectie['Adresa'].trim(),
        Locality: sectie['Localitatea'].trim(),
        Institution: sectie['Localitatea'].trim(),
    }))
}

async function solicitaLocatiePentruAdrese(sectii) {
    const sectiiFinale = []

    for await (const sectie of sectii) {
        const { Id, PollingStationNumber, County, Address, Locality, Institution } = sectie

        const queryUrl = tomUrl(`${County} ${Locality} ${Address}`)
        const response = await httpsFetch(queryUrl)
        const position = response?.results?.[0]?.position

        if (position) {
            console.log(PollingStationNumber, position)
        } else {
            console.log(PollingStationNumber, response?.summary?.queryType)
        }

        sectiiFinale.push({
            Id,
            PollingStationNumber,
            // Defaults to Chisinau
            Latitude: position?.lat ?? '46.9999648',
            Longitude: position?.lon ?? '28.7881368',
            County,
            Address,
            Locality,
            Institution
        })

        // let's not spam the server
        await sleep(500)
    }

    return sectiiFinale
}

/**
 * Ajusteaza statul pentru sectiile consecutive din acelasi stat
 * 
 * Input
 * 
 * Canada;38/7;or. Ottawa, ON (sediul misiunii);275 Slater Street Suite 801 K1P5H9, Ottawa, ON
 * ;38/8;or. Calgary, AB;North Glenmore Park Community Association, 2231 Longridge Drive SW Calgary, AB, T3E 5N5
 * 
 * Output
 * 
 * Canada;38/7;or. Ottawa, ON (sediul misiunii);275 Slater Street Suite 801 K1P5H9, Ottawa, ON
 * Canada;38/8;or. Calgary, AB;North Glenmore Park Community Association, 2231 Longridge Drive SW Calgary, AB, T3E 5N5
 */
async function ajustareStatulPentruSectiiConsecutive(sectii) {
    const sectiiAjustate = []

    for (let i = 0; i < sectii.length; i++) {
        const sectie = sectii[i]
        sectiiAjustate.push({
            ...sectie,
            County: sectie.County.trim() ? sectie.County : sectiiAjustate[i - 1].County,
        })
    }

    return sectiiAjustate
}


function tomUrl(query) {
    const api = process.env.TOMTOM_API
    return `https://api.tomtom.com/search/2/geocode/${encodeURIComponent(query)}.json?key=${api}&language=ro-RO`
}

function httpsFetch(url) {
    return new Promise((resolve, reject) => {
        https.get(url, res => {
            res.setEncoding("utf8")
            let body = ""

            res.on("data", data => {
                body += data
            })

            res.on("end", () => resolve(JSON.parse(body)))
            res.on("error", (e) => reject(e))
        })
    })
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms))
}

function asyncPipe(...functions) {
    return input => functions.reduce((chain, func) => chain.then(func), Promise.resolve(input))
}

function creazaGeoJson(sectii) {
    return {
        type: "FeatureCollection",
        features: sectii.map(sectie => ({
            type: "Feature",
            properties: {
                type: "node",
                id: sectie.PollingStationNumber,
                name: `${sectie.PollingStationNumber}: ${sectie.County} ${sectie.Locality} ${sectie.Address}`,
            },
            geometry: {
                type: "Point",
                coordinates: [
                    sectie.Longitude,
                    sectie.Latitude
                ]
            }
        }))
    }
}