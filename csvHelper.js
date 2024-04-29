const fs = require("fs");
const fsp = require("fs").promises;
const readline = require("readline");

const columnsToRemove = [
  'statutDiffusionEtablissement',
  'trancheEffectifsEtablissement',
  'anneeEffectifsEtablissement',
  'activitePrincipaleRegistreMetiersEtablissement',
  'etablissementSiege',
  'nombrePeriodesEtablissement',
  'complementAdresseEtablissement',
  'numeroVoieEtablissement',
  'indiceRepetitionEtablissement',
  'dernierNumeroVoieEtablissement',
  'indiceRepetitionDernierNumeroVoieEtablissement',
  'libelleCommuneEtrangerEtablissement',
  'distributionSpecialeEtablissement',
  'codeCedexEtablissement',
  'libelleCedexEtablissement',
  'codePaysEtrangerEtablissement',
  'libellePaysEtrangerEtablissement',
  'identifiantAdresseEtablissement',
  'coordonneeLambertAbscisseEtablissement',
  'coordonneeLambertOrdonneeEtablissement',
  'complementAdresse2Etablissement',
  'numeroVoie2Etablissement',
  'indiceRepetition2Etablissement',
  'typeVoie2Etablissement',
  'libelleVoie2Etablissement',
  'codePostal2Etablissement',
  'libelleCommune2Etablissement',
  'libelleCommuneEtranger2Etablissement',
  'distributionSpeciale2Etablissement',
  'codeCommune2Etablissement',
  'codeCedex2Etablissement',
  'libelleCedex2Etablissement',
  'codePaysEtranger2Etablissement',
  'libellePaysEtranger2Etablissement',
  'enseigne1Etablissement',
  'enseigne2Etablissement',
  'enseigne3Etablissement',
  'denominationUsuelleEtablissement',
  'activitePrincipaleEtablissement',
  'nomenclatureActivitePrincipaleEtablissement',
  'caractereEmployeurEtablissement'];

async function split(CSVpath) {
  let header;
  let indexesToKeep;
  let linesPerFile = 35000;
  let currentLine = 0;
  let currentFile = 1;
  let data = "";

  const fileStream = fs.createReadStream(CSVpath);

  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity,
  });

  for await (const line of rl) {
    if (!header) {
      header = line.split(',');
      indexesToKeep = header
        .map((col, index) => ({ col, index }))
        .filter(({ col }) => !columnsToRemove.includes(col))
        .map(({ index }) => index);
    }

    //data += line + "\n";
    data += removeColumns(line, indexesToKeep)+"\n";
    currentLine++;

    // End of stream hasn't reached the max number of lines
    if (!line && currentLine !== linesPerFile) {
      fs.appendFileSync(`csv/${currentFile}.csv`, data);
    }

    if (currentLine === linesPerFile) {
      // append data for current file
      fs.appendFileSync(`csv/${currentFile}.csv`, data);
      data = "";
      currentLine = 0;
      currentFile++;
    }
  }
}

function removeColumns(line, indexesToKeep) {
    // Filtrage des lignes pour ne garder que les colonnes désirées
    const filteredLines = line.split(',').filter((col, index) => indexesToKeep.includes(index)).join(',');

    return filteredLines
}

function countFilesInDirectory(directoryPath) {
    console.log('startcount');
    
    // Lecture du contenu du dossier
    return fsp.readdir(directoryPath)
        .then(files => {
            // Filtrage des fichiers pour ne garder que les fichiers (pas les dossiers)
            const fileCount = files.filter(file => fs.statSync(`${directoryPath}/${file}`).isFile()).length;
            return fileCount;
        })

}

module.exports = {split, countFilesInDirectory};





