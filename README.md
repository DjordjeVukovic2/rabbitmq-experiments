# Asinhrona komunikacija sa RabbitMQ-om, aplikacija i eksperimenti


Napravljena je mala aplikacija (producer i consumer servis), a zatim je na njoj
mjereno kako se asinhroni sistem ponaša: kako skalira, kako se ponaša kad consumer padne, 
šta se dešava s porukama koje uvijek padaju obradu i koliko trajnost poruka košta.  

Aplikacija je napravljena u Spring Bootu i koristi RabbitMQ za razmjenu poruka. 
Metrike se čuvaju u Prometheusu, a prikazuju u Grafani. Sve se pokreće preko Dockera.



### Ideja aplikacije

Producer servis prima zahtjeve i ubacuje ih u red, a Consumer (ili više njegovih primjeraka) 
ih obrađuje nezavisno i u svom tempu. Producer ne čeka da obrada završi, a obrada se skalira dodavanjem 
instanci consumera bez ikakve izmjene producera.

Obrada je namjerno malo duži posao, da bi se skaliranje moglo mjeriti. Svaka poruka se
obrađuje BCrypt hashiranjem (oko 50–70 ms, težina se može prilagoditi).


### task-producer-service

Servis koji ubacuje poruke u red tasks. Jedan endpoint:

POST /publish?count=N[&poison=K][&persistent=true|false][&confirm=true|false]

- count – broj poruka koje se šalju.
- poison – (opciono) koliko od njih su otrovne poruke koje consumer uvijek odbija (eksperiment 4).
- persistent – (opciono) način isporuke, persistent ili non-persistent.
- confirm – (opciono) ako je true, producer čeka potvrdu brokera za sve poruke (publisher confirms).

Poruka je JSON objekat: TaskMessage { id (UUID), payload (tekst), sentAt (vrijeme slanja) }.
Red tasks je trajan (durable).

### task-consumer-service

Servis koji uzima poruke iz reda i za svaku poruku računa BCrypt hash i izlaže metrike.
Ponašanje se mijenja varijablama okruženja, pa se isti image koristi u svim eksperimentima:

- PREFETCH – koliko neobrađenih poruka broker smije unaprijed dati jednom consumeru. 
- CONCURRENCY – broj thread-ova po instanci.
- BCRYPT_COST – težina hasha (svaka jedinica udvostručuje težinu)
- REQUEUE – da li se odbijena poruka vraća u red. 

Metrike se mjere pomoću biblioteke Micrometer, a Prometheus ih skuplja preko Actuator endpointa /actuator/prometheus.

- tasks_processed_total – brojač obrađenih poruka po instanci.
- tasks_hash_seconds – trajanje jednog hasha.
- tasks_queue_wait_seconds – koliko je poruka čekala u redu prije obrade.
- rabbitmq_queue_messages_ready, rabbitmq_queue_messages_unacked – broj poruka u redu koje čekaju i koje su isporučene.

## Eksperimenti koje sam probao

Eksperimenti se pokreću pomoću PowerShell skripti (nalaze se u folderu monitoring), koje se brinu da svaki prolaz kreće iz istog stanja:

- red se briše, consumeri se prave iznova,
- svaka konfiguracija se ponavlja u više krugova,
- rezultat se upisuje u CSV poslije svakog prolaza,
- prolaz se označava kao valjan samo ako su ispunjeni uslovi ispravnosti (npr. povezan je
  traženi broj consumera, potvrđen je traženi prefetch, nijedna poruka nije izgubljena).

CSV fajlovi za svaki eksperiment nalaze se u folderu results.


### 1. Skaliranje consumera

Koliko se propusnost povećava dodavanjem instanci consumera i gdje to prestaje da se isplati?

Mjere:
- Propusnost – koliko poruka sistem obradi u sekundi
- Ubrzanje – koliko je puta sistem brži sa N instanci nego sa jednom: propusnost(N) / propusnost(1)
- Efikasnost – koliki dio idealnog ubrzanja se stvarno dobije: ubrzanje / N
- Hash – prosječno trajanje jednog hasha u milisekundama

Eksperiment:    


Red se unaprijed napuni sa 60.000 poruka, a tek onda se pokreće 1, 2, 4, 6 ili 8 instanci consumera. Svaki consumer koristi BCrypt cost 10 i prefetch 5. Red se ne prazni do kraja, nego se mjeri brzina obrade u ustaljenom stanju: prvih 60 s se odbacuje kao zagrijavanje, a mjeri se sljedećih 300 s. Iz izmjerene brzine se zatim procjenjuje koliko bi trajalo pražnjenje reda od 150.000 poruka. Svaka konfiguracija se izvršava u 3 kruga, uz pauzu od 60 s između prolaza.


Rezultati (vrijednosti su prosjek 3 kruga)

| Instanci | Propusnost (msg/s) | Ubrzanje | Efikasnost | Hash (ms) | Procjena za 150.000 poruka |
|---|---|---|---|---|---|
| 1 | 17,68 ± 0,22 | 1,00 | 100% | 56,4 | 141 min |
| 2 | 34,35 ± 0,27 | 1,94 | 97,1% | 58,0 | 73 min |
| 4 | 62,01 ± 0,32 | 3,51 | 87,7% | 64,2 | 40 min |
| 6 | 82,89 ± 0,52 | 4,69 | 78,2% | 72,0 | 30 min |
| 8 | 99,33 ± 0,41 | 5,62 | 70,2% | 80,1 | 25 min |

Broj iza ± je standardna devijacija, tj. mjera koliko su se krugovi međusobno razlikovali. U ovom slučaju razlike su male, pa se može vjerovati da su izmjerene brojke pouzdane.

Zaključak   

Skaliranje je gotovo linearno do 2 instance, a zatim efikasnost opada, pa 8
instanci daje 5,6 puta veću propusnost od jedne (70% idealnog). Propusnost se poklapa sa
N × 1000 / hash (za 8 instanci: 8000ms / 80,1 = 99,9, izmjereno 99,3), što znači da
instance gotovo nikad ne čekaju poruke. Gubitak efikasnosti dolazi od usporavanja samog
hashiranja kad više instanci dijeli isti procesor (hash raste sa 56 na 80 ms).

### 2. Prefetch

Kako veličina prefetch-a utiče na pražnjenje reda kad se instance dodaju usred opterećenja?

Prefetch je broj poruka koje broker smije unaprijed dati jednom consumeru, a da on nijednu
nije potvrdio. Te poruke su za broker zauzete, pa ih ne može dati drugom consumeru.

Postavke

Red se unaprijed napuni sa 20.000 poruka. Zatim se pokreću 4 instance consumera, a nakon 20 s se dodaju još 4, tako da ih ukupno radi 8. Isti scenario se ponavlja za prefetch 1, 50, 500, 2500 i 5000. Za razliku od eksperimenta skaliranja, ovdje se mjeri cijelo pražnjenje reda, a ne samo ustaljeno stanje. Svaka vrijednost prefetch-a izvršava se u 3 kruga, uz pauzu od 60 s između prolaza.

Mjere

- Drain – ukupno vrijeme pražnjenja reda u sekundama.
- Propusnost – prosječna brzina cijelog pražnjenja. broj poruka / drain.
- Idle – udio ukupnog kapaciteta koji je propao zato što su neke instance mirovale dok su druge još radile.
- Min / Max po instanci – najmanji i najveći broj poruka koje je obradila jedna instanca.
- CV – koliko je raspodjela posla među instancama neravnomjerna. standardna devijacija / srednja vrijednost.

Rezultati (prosjek tri kruga)

| Prefetch | Drain (s) | Propusnost (msg/s) | Idle | Min / Max po instanci | CV |
|---|---|---|---|---|---|
| 1 | 217 ± 11 | 91,4 | 0,4% | 2177 / 2822 | 13,0% |
| 50 | 220 ± 5 | 90,1 | 1,3% | 2182 / 2833 | 13,0% |
| 500 | 222 ± 4 | 89,5 | 0,7% | 2204 / 2817 | 12,2% |
| 2500 | 237 ± 8 | 84,0 | 11,0% | 159 / 3104 | 39,5% |
| 5000 | 334 ± 5 | 59,5 | 44,7% | 0 / 5018 | 106,9% |

Zaključak  

Dok je prefetch × broj početnih instanci mali dio reda (do 4 × 500 = 2000),
prefetch ne utiče na vrijeme. Prelom nastaje kad početne instance mogu zadržati veći dio reda:
pri 2500 četiri početne instance drže pola reda, a pri 5000 cijeli red, pa nove instance
dobiju nula poruka i pola kapaciteta miruje (44,7%). Propusnost tad pada na oko 60 msg/s, što je tačno kapacitet četiri instance (4 × 1000 / 66 ms).   
prefetch × broj consumera mora ostati
mali dio reda, inače veliki prefetch poništava skaliranje.

### 3. Pad consumera usred rada

Šta se desi s porukama koje je consumer držao kad se nasilno ugasi, i da li se išta izgubi?

Poruka je potvrđena (ack) kad consumer javi da je završio obradu, a unacked ako je
isporučena, ali nije potvrđena. Kad consumer padne, broker sve njegove unacked poruke vraća u red i isporučuje ih drugima (redelivery). 

Postavke

Red se napuni sa 10.000 poruka i pokreću se 4 instance consumera. 30 sekundi poslije starta jedan consumer se nasilno ubija, a preostala tri nastavljaju da prazne red. Isti scenario se ponavlja za prefetch 1, 50, 500 i 2500, u 2 kruga.

Rezultati

| Prefetch | Unacked prije pada | Ponovo isporučeno (krug 1 / 2) | Poslano = potvrđeno |
|---|---|---|---|
| 1 | 4 | 1 / 1 | 10.000 = 10.000 |
| 50 | 200 | 50 / 50 | 10.000 = 10.000 |
| 500 | 2.000 | 500 / 500 | 10.000 = 10.000 |
| 2500 | ~7.800–7.950 | 1.880 / 1.945 | 10.000 = 10.000 |

Zaključak     

Ni u jednom od 8 prolaza nije izgubljena nijedna poruka. Broj ponovo isporučenih
poruka je tačno prefetch ubijenog consumera. Kod 2500 je ponovo isporučeno manje od 2500 poruka (1880 i 1945). Četiri consumera su na početku zajedno preuzela cijeli red, pa broker nije imao poruka da im dopuni prefetch. Kako je consumer obrađivao poruke, broj poruka koje drži se smanjivao, pa je do pada obradio oko 550–620 poruka i vratio u red samo preostale.

### 4. Poruka koja uvijek pada (poison) i mrtvi red

Šta se desi s porukom koju nijedan consumer ne može obraditi, i kako postavke odlučuju
da li se ona izgubi, vrti u krug ili sačuva?

Poison poruka je poruka čija obrada uvijek baca grešku. Kad listener baci izuzetak,
poruka se odbija, a postavka requeue određuje da li se vraća u red. Mrtvi red (DLQ) je red u koji broker preusmjerava odbijene poruke umjesto da ih odbaci.

Postavke

U red se ubaci 3.000 poruka, od kojih je 10 poison, ravnomjerno raspoređenih kroz red. Obrađuju ih 4 instance consumera. Isti scenario se pušta kroz tri varijante ponašanja pri odbijanju poruke (requeue, drop i dlq), a cijela serija se ponavlja u 2 kruga.

Ispitane su tri varijante:

- requeue (default) - odbijena poruka se vraća u red i odmah ponovo isporučuje.
- drop - odbijena poruka se odbacuje, bez mrtvog reda.
- dlq - odbijena poruka se uklanja iz reda i završava u mrtvom redu tasks.dlq.


Rezultati (isti u oba kruga)

| Varijanta | Zdrave potvrđene | Ponovo isporučeno | U redu na kraju | U DLQ | Izgubljeno |
|---|---|---|---|---|---|
| requeue | 2.990 | **452.535 / 464.690** | 10 | 0 | 0 |
| drop | 2.990 | 0 | 0 | 0 | **10** |
| dlq | 2.990 | 0 | 0 | **10** | 0 |

Zakljucak  

Varijanta requeue pretvara 10 loših poruka u neprekidnu petlju, otprilike
6.000 isporuka u sekundi, i red se nikad ne isprazni. Varijanta drop prekida petlju, ali gubi poruke tj. 10 poruka nestane. Varijanta sa mrtvim redom ne gubi
ništa i ne vrti u krug, a poruke ostaju dostupne za analizu ili ponovnu obradu.

### 5. Preživljavanje poruka pri padu brokera

Šta se desi s porukama u redu kad padne sam RabbitMQ, i odlučuje li način isporuke o tome?

Persistent poruka se zapisuje na disk, a non-persistent živi samo u memoriji.
Trajan (durable) red preživi restart, ali samo kao prazna struktura.

Postavke

U red se ubaci 20.000 poruka, a consumera nema, pa sve poruke ostaju u redu. Zatim se broker obori na jedan od dva načina: docker restart ili nasilni pad docker kill, pa ponovno pokretanje. Poruke se šalju kao persistent ili kao non-persistent, što daje četiri kombinacije, a svaka se ponavlja u 3 kruga. Mjeri se broj poruka u redu prije pada i poslije oporavka brokera.

Rezultati (isti u sva tri kruga)

| Isporuka | Način pada | Prije | Poslije | Izgubljeno |
|---|---|---|---|---|
| persistent | restart | 20.000 | 20.000 | 0 |
| persistent | kill | 20.000 | 20.000 | 0 |
| non-persistent | restart | 20.000 | 0 | 20.000 |
| non-persistent | kill | 20.000 | 0 | 20.000 |

Zaključak  

 Trajan red preživi pad brokera, ali non-persistent poruke u njemu ne prežive nijedan od
načina pada, dok persistent prežive oba. Trajnost reda ne čuva poruke, nego samo trajnost poruka.
Vrijeme dok broker ponovo počne odgovarati je 12–14 s u svim slučajevima.

### 6. Cijena trajnosti u brzini

Koliko persistent poruke usporavaju prijem i obradu u odnosu na non-persistent?

Publisher confirm je potvrda koju broker šalje produceru da je primio poruke. Za persistent poruke u trajnom redu stiže tek kad je poruka zapisana na disk. Bez ove potvrde bi se mjerilo samo koliko brzo producer šalje poruke, a ne koliko brokeru treba da ih primi i zapiše, pa takvo vrijeme ne pokazuje cijenu trajnosti.

Postavke

Eksperiment ima dvije faze, a svaki prolaz prolazi kroz obje. U prvoj fazi (prijem) producer šalje 500.000 poruka i čeka da broker sve potvrdi, a consumera nema. U drugoj fazi (obrada) 8 consumera prazni red. U ovom eksperimentu hash je namjerno mnogo manji (oko 1,5 ms umjesto oko 70 ms). Da je obrada spora, consumeri bi bili usko grlo, pa se razlika između persistent i non-persistent poruka ne bi mogla vidjeti.

Isti scenario se ponavlja za persistent i non-persistent poruke, u 5 krugova, a redoslijed varijanti se mijenja iz kruga u krug. Prije serije rade se dva prolaza zagrijavanja koja se ne bilježe. 

Rezultati (prosjek 5 prolaza)

| | persistent | non-persistent |
|---|---|---|
| Prijem sa potvrdom (s) | 14,7 ± 0,4 | 15,5 ± 1,2 |
| Prijem (msg/s) | 34.122 ± 826 | 32.512 ± 2.831 |
| Pražnjenje (s) | 103,7 ± 4,1 | 105,9 ± 1,0 |
| Obrada (msg/s) | 4.828 ± 203 | 4.721 ± 44 |
| Hash (ms) | 1,57 | 1,59 |

Broj iza ± je standardna devijacija, tj. mjera koliko su se krugovi međusobno razlikovali.

Zaključak  

Razlike između persistent i non-persistent poruka su manje od razlika između pojedinačnih prolaza iste varijante, pa se ne mogu pripisati načinu isporuke. Brzinu pražnjenja reda određuju consumeri, jer su radili gotovo punom brzinom (oko 94% svog maksimuma), pa broker nije bio usko grlo. U probnom prolazu bez zagrijavanja persistent je izgledao oko 28% sporije, ali ta razlika je nestala u seriji, pa je to vjerovatno posljedica zagrijavanja producera.