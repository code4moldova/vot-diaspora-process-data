import fs from 'fs'
import https from 'https'

const fieldDelimiter = `;`
const stringDelimiter = `"`

const source = fs.readFileSync('./input.csv', 'utf8')
const [_head, ...sectii] = source.split('\n')

const procesareSectii = asyncPipe(
    eliminaSpatiiColoane,
    ajustareStatulPentruSectiiConsecutive,
    ordonareColoanePentruBazaDeDate,
    solicitaLocatiePentruAdrese,
    eliminaSpatiiColoane,
    fixeazaGhilimele,
    eliminaSpatiiColoane,
)

const sectiiFinale = await procesareSectii(sectii)

const headers = [
    'PollingStationNumber',
    'Latitude',
    'Longitude',
    // Yes it is `County` there, this is DB column name :)
    'County',
    'Address',
    'Locality',
    'Institution'
].join(fieldDelimiter)

const output = `${headers}\n${sectiiFinale.join('\n')}`

fs.writeFileSync('./output.csv', output, { encoding: 'utf8' })

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

async function ordonareColoanePentruBazaDeDate(sectii) {
    return sectii.map(sectie => {
        const [Country, PollingStationNumber, Locality, Address] = sectie.split(fieldDelimiter)

        return [
            PollingStationNumber,
            'NaN', // Latitude
            'NaN', // Longitude
            Country,
            Address,
            Locality, // Locality
            Locality // Institution
        ].join(fieldDelimiter)
    })
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
        if (sectie.startsWith(fieldDelimiter)) {
            const sectiePrecedenta = sectiiAjustate[i - 1]
            const [misiunea] = sectiePrecedenta.split(fieldDelimiter)
            const sectieAjustata = `${misiunea}${sectie}`
            sectiiAjustate.push(sectieAjustata)
        } else {
            sectiiAjustate.push(sectie)
        }
    }

    return sectiiAjustate
}

async function eliminaSpatiiColoane(sectii) {
    return sectii.map(sectie => sectie
        .split(fieldDelimiter)
        .map(coloana => coloana.trim())
        .join(fieldDelimiter)
    )
}

async function fixeazaGhilimele(sectii) {
    return sectii.map(sectie => sectie
        .split(fieldDelimiter)
        .map(coloana => existaGhilimeleDoarInParti(coloana)
            ? eliminaGhilimele(coloana)
            : coloana
        )
        .join(fieldDelimiter)
    )
}

function eliminaGhilimele(coloana) {
    return coloana.replace(new RegExp(stringDelimiter, 'g'), '')
}

function existaGhilimeleDoarInParti(coloana) {
    const startsEnds = coloana.startsWith(stringDelimiter) && coloana.endsWith(stringDelimiter)
    return startsEnds && (coloana.match(new RegExp(stringDelimiter, 'g')) || []).length === 2
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

