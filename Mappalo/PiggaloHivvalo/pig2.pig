
dati = LOAD '/Users/Stefano/Documents/BigData/DataGenerator/data/esempio.txt' USING TextLoader() AS (line:chararray);

trimestre = FOREACH (filter dati by  (INDEXOF($0, '-1-')!=-1 OR INDEXOF($0, '-2-')!=-1 OR INDEXOF($0, '-3-')!=-1)) GENERATE $0;

/*ELimino il giorno e le virgole. Per le virgole è il secondo FOREACH*/
senza_giorno_virgole = FOREACH (FOREACH trimestre GENERATE CONCAT(CONCAT(SUBSTRING($0, 0, 6), ','), $0)) GENERATE FLATTEN(STRSPLIT($0, ',')) ;

 /*Creo Data,Prodotti.Dall'output si vede che ci sono anche le coppie di date.*/
dataEprodotto_temp = FOREACH senza_giorno_virgole GENERATE $0 AS data, FLATTEN(TOBAG(*)) as value;

 /*ELimino la coppia di date'*/
dataEprodotto = FOREACH (FILTER dataEprodotto_temp by INDEXOF(value, '-') == -1) GENERATE  value as prodotto, data;

 /*Raggruppo*/
group_dataEprodotto = GROUP dataEprodotto BY (prodotto, data);

/*Conteggio dei prodotti.*/
conteggio = FOREACH group_dataEprodotto GENERATE group, COUNT(dataEprodotto) as quantity;

 /*Creo una cosa leggibile per capire come procedere al passo successivo. Probabilmente userò il campo prodotti per raggrupparle*/
tabella = FOREACH conteggio GENERATE $0.prodotto as prodotto, $0.data as data,  $1 as conteggio_prodotto;

 /*prodotto-data-quantità. Volendo è già un output valido, ma non è nel formato che chiede il prof*/
risultato_temp = GROUP tabella BY prodotto;

/*Creo il formato richiesto. Creo per ogni prodotto i campi data-quantità per il trimestre*/
risultato= FOREACH risultato_temp {data_quantita = FOREACH tabella GENERATE data, conteggio_prodotto;GENERATE group, data_quantita;}; 

/*scrivo il risultato*/
store risultato into '/Users/Stefano/Documents/pig-0.14.0/pigoutput' using PigStorage();