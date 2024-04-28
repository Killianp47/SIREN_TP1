# SIREN_TP1

#  Topographie du cluster nosql

Le schéma est disponible dans le repository sous le nom topologie.drawio.png

#  Doagramme activité

Le schéma est disponible dans le repository sous le nom diagrammeActivité.drawio.png

# Téléchargement du Fichier

Un fois le fichier csv télécharger, il faut le déposer dans le projet, et vérifier que le répertoire "data" est présent. Si il n'est pas présent, créer le repertoire "data".

## Installation des packages

    npm -i pm2@latest
	npm i -g pm2
	npm -i fs@latest
	npm -i mongoose

##  Démarrage

    npm run start


## Commande pm2
Afficher la liste des workers

    pm2 list

Supprimer les workers

    pm2 delete all

