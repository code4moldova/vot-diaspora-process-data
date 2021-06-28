import fs from 'fs'
import https from 'https'
import parse from 'csv-parse/lib/sync.js'
import stringify from 'csv-stringify/lib/sync.js';

const fieldDelimiter = `;`
const stringDelimiter = `"`
const source = fs.readFileSync('./input.csv')

const sectii = parse(source, {
    columns: true,
    trim: true,
    skip_empty_lines: true,
    delimiter: fieldDelimiter,
    escape: stringDelimiter
})

const procesareSectii = asyncPipe(
    remapareColoane,
    ajustareStatulPentruSectiiConsecutive,
    // solicitaLocatiePentruAdrese,
)

const sectiiFinale = await procesareSectii(sectii)

const output = stringify(sectiiFinale, {
    header: true,
    delimiter: fieldDelimiter,
    quote
})

fs.writeFileSync('./output.csv', output, { encoding: 'utf8' })

async function remapareColoane(sectii) {
    return sectii.map(sectie => ({
        PollingStationNumber: sectie['Nr. SV'],
        Latitude: '',
        Longitude: '',
        // Yes it is `County` there, this is DB column name :)
        County: sectie['Misiunea / Statul'],
        Address: sectie['Adresa'],
        Locality: sectie['Localitatea'],
        Institution: sectie['Localitatea'],
    }))
}

async function solicitaLocatiePentruAdrese(sectii) {
    const sectiiFinale = []

    for await (const sectie of sectii) {
        const [
            PollingStationNumber,
            _FakeLatitude,
            _FakeLongitude,
            Country,
            Address,
            Locality,
            Institution
        ] = sectie.split(fieldDelimiter)

        const queryUrl = tomUrl(`${Country} ${Locality} ${Address}`)
        const response = await httpsFetch(queryUrl)

        const position = response?.results?.[0]?.position

        if (position) {
            console.log(PollingStationNumber, position)
        } else {
            console.log(PollingStationNumber, response?.summary?.queryType)
        }

        const statieCuLocatie = [
            PollingStationNumber,
            position?.lat ?? '',
            position?.lon ?? '',
            Country,
            Address,
            Locality,
            Institution
        ]
        sectiiFinale.push(statieCuLocatie.join(fieldDelimiter))

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

