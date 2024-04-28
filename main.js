const fs = require('fs');
const pm2 = require('pm2');
const performance = require('perf_hooks');
const csvVersTableau = require('./splitcsv');
const mongodb = require("./database");
const os = require('os');

const CHEMIN_FICHIER = 'StockEtablissement_utf8.csv';
const TRAVAIL_EN_COURS = {};
const TRAVAIL_EN_ATTENTE = [];
const INSTANCES_LIBRES = [];
const FILTRE = [];
const NOMBRE_DE_NOEUDS = 1;
const tempsDebut = performance.performance.now();
const NOMBRE_CPU = os.cpus().length;

async function démarrerTraitement() {
    await connexionBaseDonnées();
    await démarrerClusterTravailleurs();
    await diviserEtTraiterCSV();
}

async function connexionBaseDonnées() {
    await mongodb.createConnection(true);
}

async function démarrerClusterTravailleurs() {
    for (let i = 0; i < NOMBRE_DE_NOEUDS; ++i) {
        démarrerTravailleur(i);
    }
}

function démarrerTravailleur(index) {
    pm2.start({
        script: 'worker.js',
        name: `worker${index}`,
        instances: 'max',
    }, (err, _) => {
        if (err) console.error(err);
    });
}

async function diviserEtTraiterCSV() {
    await diviserCSV();
    console.log("Division CSV terminée !");
}

async function diviserCSV() {
    const tailleMorceauEnMo = 30;
    const tailleMorceau = 1024 * 1024 * tailleMorceauEnMo;
    const tampon = Buffer.alloc(tailleMorceau);
    const descripteurFichier = fs.openSync(CHEMIN_FICHIER, 'r');
    let décalage = 0;
    let idCSV = 0;

    while (true) {
        const octetsLus = fs.readSync(descripteurFichier, tampon, 0, tailleMorceau, décalage);
        if (octetsLus === 0) break;

        const finFlux = tampon.lastIndexOf("\n", octetsLus);
        if (finFlux < 0) throw "Le fichier est trop petit ou le fichier n'est pas un CSV.";

        let débutFlux = 0;
        if (FILTRE.length === 0) {
            débutFlux = tampon.indexOf("\n", 0);
            FILTRE.push(...extraireEnTêtes(tampon.slice(0, débutFlux).toString()));
        }

        const cheminSauvegarde = `./data/CSV-${idCSV++}.csv`;
        await sauvegarderMorceau(tampon, débutFlux + 1, finFlux, cheminSauvegarde);
        décalage += finFlux + 1;
    }
}

async function sauvegarderMorceau(tampon, début, fin, cheminSauvegarde) {
    await fs.writeFileSync(cheminSauvegarde, tampon.slice(début, fin));
    TRAVAIL_EN_ATTENTE.push(cheminSauvegarde);
}

function extraireEnTêtes(enTête) {
    const csv = csvVersTableau(enTête);
    if (csv.length !== 1) throw "Aucun en-tête trouvé dans le fichier.";
    const enTêteTableau = csv[0];

    const enTêtesRecherchés = [
        "siren",
        "nic",
        "siret",
        "dateCreationEtablissement",
        "dateDernierTraitementEtablissement",
        "typeVoieEtablissement",
        "libelleVoieEtablissement",
        "codePostalEtablissement",
        "libelleCommuneEtablissement",
        "codeCommuneEtablissement",
        "dateDebut",
        "etatAdministratifEtablissement"
    ];

    return enTêtesRecherchés.map(enTêteRecherché => {
        const idx = enTêteTableau.findIndex(colonne => colonne.includes(enTêteRecherché));
        if (idx === -1) throw "En-tête non trouvé : " + enTêteRecherché;
        return idx;
    });
}

function gérerMessage(packet) {
    console.log(packet);
    mettreÀJourTravailleurs(packet);
}

function mettreÀJourTravailleurs(packet) {
    const { PID, READY } = packet.data;

    if (READY) {
        traiterTravailleurPrêt(PID);
    } else {
        traiterTravailleurTerminé(PID, packet.data.FILE, packet.error);
    }

    if (TRAVAIL_EN_ATTENTE.length > 0 && INSTANCES_LIBRES.length > 0 && FILTRE.length > 0) {
        envoyerTravaux();
    }
}

function traiterTravailleurPrêt(PID) {
    INSTANCES_LIBRES.push(PID);
    console.log("Machine prête : " + PID);

    if (TRAVAIL_EN_ATTENTE.length > 0) {
        envoyerTravaux();
    }
}

function traiterTravailleurTerminé(PID, fichier, erreur) {
    INSTANCES_LIBRES.push(PID);
    const délai = performance.performance.now() - TRAVAIL_EN_COURS[fichier];
    console.log("Temps écoulé : " + délai + " ms pour " + fichier);
    console.log(INSTANCES_LIBRES.length + " instances libres.");

    const tempsTotalÉcoulé = performance.performance.now() - tempsDebut;
    console.log("Temps total écoulé : " + tempsTotalÉcoulé/60000 + " mins");

    if (INSTANCES_LIBRES.length === NOMBRE_CPU * NOMBRE_DE_NOEUDS) {
        const executionTime = performance.performance.now() - tempsDebut;
        console.log("Données chargées avec succès dans la base de données. Appuyez sur Crtl+C pour quitter et 'pm2 delete all' pour arrêter tous les processus");
        console.log("Temps d'exécution : " + executionTime + " millisecondes");
    }

    if (TRAVAIL_EN_ATTENTE.length > 0) {
        envoyerTravaux();
    }
}

function envoyerTravaux() {
    const nombreInstancesDisponibles = Math.min(TRAVAIL_EN_ATTENTE.length, INSTANCES_LIBRES.length);

    for (let i = 0; i < nombreInstancesDisponibles; ++i) {
        const PID = INSTANCES_LIBRES.shift();
        const travail = TRAVAIL_EN_ATTENTE.shift();

        TRAVAIL_EN_COURS[travail] = performance.performance.now();
        console.log("Envoi du travail à " + PID + " : " + travail);

        pm2.sendDataToProcessId(PID, {
            type: 'process:msg',
            data: {
                FILE: travail,
                FILTER: FILTRE
            },
            topic: "SIRENE-INVADER"
        }, (error, result) => {
            if (error) console.error(error);
        });
    }
}

pm2.connect(async function (err) {
    if (err) {
        console.error(err)
        process.exit(2)
    }

    pm2.launchBus(function (err, pm2_bus) {
        pm2_bus.on('process:msg', gérerMessage);
    });

    try {
        if (fs.existsSync(CHEMIN_FICHIER)) {
            await démarrerTraitement();
        } else {
            console.error(`Le fichier ${CHEMIN_FICHIER} n'existe pas.`);
        }
    } catch (err) {
        console.error(err)
    }
});
