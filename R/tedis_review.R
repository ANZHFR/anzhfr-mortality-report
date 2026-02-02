tedis_review <- function(raw_data) {
  edsi_ids <- c(
    "au020180",
    "au028556",
    "au028555",
    "au028561",
    "au028635",
    "au028633",
    "au028638",
    "au028637",
    "au028642",
    "au028648",
    "au028649",
    "au029928",
    "au029941",
    "au029920",
    "au030037",
    "au036672",
    "au036827",
    "au036829",
    "au038253",
    "au036830",
    "au036832",
    "au036831",
    "au038255",
    "au038258",
    "au038254",
    "au038256",
    "au038257",
    "au038264",
    "au038259",
    "au038260",
    "au038261",
    "au038263",
    "au038262",
    "au038265",
    "au041390",
    "au041392",
    "au041396",
    "au041395",
    "au041393",
    "au041397",
    "au041394",
    "au041402",
    "au041399",
    "au041398",
    "au041400",
    "au041401",
    "au042437",
    "au042438",
    "au042439",
    "au042445",
    "au042442",
    "au042446",
    "au042440",
    "au042441",
    "au042444",
    "au042447",
    "au042443",
    "au042448",
    "au046291",
    "au067448",
    "au067449",
    "au067450",
    "au067451",
    "au067452",
    "au067453",
    "au067454",
    "au067456",
    "au067457",
    "au067458",
    "au067459",
    "au067471",
    "au067472",
    "au067477",
    "au067478",
    "au103036",
    "au103037",
    "au103172",
    "au103005",
    "au103173",
    "au102912",
    "au103174",
    "au103038",
    "au103175",
    "au103039",
    "au103041",
    "au103040",
    "au103042",
    "au103043",
    "au103007",
    "au087211",
    "au096948",
    "au092570",
    "au093652",
    "au095142",
    "au095020",
    "au094526",
    "au097681",
    "au100536",
    "au108371",
    "au110836",
    "au114113"
  )

  esd_ids <- c(
    "au002445",
    "au004618",
    "au004291",
    "au004635",
    "au005080",
    "au007094",
    "au009725",
    "au011182",
    "au012294",
    "au013084",
    "au012539",
    "au026416",
    "nz007888",
    "au027719",
    "au024958",
    "au041840",
    "nz012652",
    "au027577",
    "au035596",
    "au036045",
    "au036576",
    "au053195",
    # "au046593",
    "au048687",
    "au050835",
    "au067275",
    "au066468",
    "au067884",
    "au074660",
    "au074601",
    "au076950",
    "nz031770",
    "nz031659",
    "au091671",
    "au098319",
    "au104615",
    "nz037114",
    "au107572",
    # "au113343",
    "au008737",
    "au008783",
    "au002411",
    "au003909",
    "nz003153",
    "au007890",
    # "au011450",
    "au012455",
    "au017597",
    "au019457",
    "au019460",
    "au026595",
    "au063681",
    "au072482",
    "au073311",
    "au081097",
    "au075548",
    "au081770",
    "au086289",
    "au087742",
    "au090186",
    "au099615",
    "au108529",
    "au109164",
    "au113374"
  )

  edts_ids1 <-
    c(
      "au004162",
      "au007259",
      "au030332",
      "au060497",
      "au075098",
      "au074731",
      "au101029",
      "au105472",
      "au108923",
      "au114031",
      "au012982",
      "au011330",
      "au019706",
      "au030599",
      "au036979",
      "au034212",
      "au036113",
      "au043386",
      "au052474",
      "au061030",
      "nz028870",
      "au072278",
      "au073627",
      "au075711",
      "au092045",
      "au109485",
      "au113877"
    )

  edts_ids2 <-
    c(
      "au008745",
      "au008690",
      "au008847",
      "au008850",
      "au008851",
      "au008852",
      "au008731",
      "au008710",
      "au008797",
      "au008800",
      "nz017921",
      "au057364",
      "au092740",
      "au109979",
      "au113467",
      "au008820",
      "au008821",
      "au008736",
      "au008732",
      "au008795",
      "au012948",
      # "au025426",
      "au038125",
      "au049460",
      # "au060435",
      "au070696",
      "au072823"
      # "au099497",
    )

  etds_ids <-
    c(
      "au011090",
      "au013040",
      "au013058",
      "au013061",
      "au013140",
      "au013141",
      "au013355",
      "au013359",
      "au013362",
      "au013876",
      "au015235",
      "au016026",
      "au014712",
      "au046056",
      "au049201",
      "au049775",
      "au067053",
      "au057068",
      "au056106",
      "au067234",
      "au061421",
      "au063155",
      "au063033",
      "au063165",
      "au063161",
      "au070420",
      "au073730",
      "au120552",
      "au009203",
      "au009222",
      "au009710",
      "au012987",
      "au013045",
      "au013067",
      "au020251",
      "au017265",
      "au026488",
      "au047818",
      "au049106",
      "au050314",
      "au056628",
      "au060102",
      "au063035",
      "au073499",
      "au070745",
      "au083589",
      "au073594",
      "au074079",
      "au074385",
      "au076344",
      "au087013",
      "au091550",
      "au103432",
      "au120568"
    )

  tsed_ids <-
    c(
      "au007839",
      "nz006848",
      "au015095",
      "au019294",
      "au020685",
      "nz016081",
      "au040257",
      "au048281",
      "au064280",
      "nz027882",
      "au066529",
      "au069429",
      "nz036413",
      "nz037191",
      "au108500",
      "au109817",
      "au108116",
      "au013565",
      "nz007329",
      "nz012595",
      "au028705",
      "au029278",
      "au032615",
      "nz021161",
      "au039918",
      "au042411",
      "nz024468",
      "au045846",
      "au046277",
      "au050908",
      "au067736",
      "au074083",
      "au076402",
      "au099097",
      "au099593",
      "nz037796",
      "au109104",
      "au110848"
    )

  ised_ids <-
    c(
      "au086756", # added after post-review on 02-Feb-2026
      "au033044",
      "au033957",
      "nz018398",
      "au040971",
      "au042882",
      "au061112",
      "au062581",
      "nz028503",
      "nz031033",
      "au094170",
      "nz032572",
      "au090669",
      "au089353",
      "nz034361",
      "au099093",
      "au095830",
      "au096677",
      "au109158",
      "au028199",
      "au034542",
      "au061424",
      "au060565",
      "au064174",
      "au067315",
      "nz028948",
      "au076765",
      "au073540",
      "au074795",
      "nz031654",
      "au097868"
    )

  ieds_ids1 <-
    c(
      "au111359", # moved from `ieds_ids2` after reviewing raw data.
      "au026604",
      "au033366",
      "au111404",
      "au043101",
      "nz035910",
      "nz035911",
      "au110825",
      "au111380",
      "au111386"
    )

  ieds_ids2 <-
    c(
      "nz025665",
      "au057045",
      "au061543",
      "au066432",
      "au085659",
      "au096073",
      "au093966",
      # "au111359", # moved to `ieds_ids1` after reviewing raw data.
      "nz038977",
      "nz002700",
      "au098026",
      "nz007122",
      "au020039",
      "au037549",
      "au033498",
      "au048069",
      "nz026859",
      "au069147",
      "au074495",
      "au082265",
      "nz037200"
    )

  edst_ids <-
    c(
      "au069417", "au098434", # identified after post-review 02-Feb-2026
      "au019385",
      "au019386",
      "au013934",
      "au016162",
      "au056224",
      "au050054",
      "au081126",
      "au083022",
      "nz033537",
      "au093895",
      "au091542",
      "au100950",
      "au013635",
      "au019026",
      "au019286",
      "au032312",
      "au036708",
      "au029557",
      "nz024585",
      "au046177",
      "au045859",
      "au059495",
      "au063543",
      "au087833",
      "au075221",
      "au075514",
      "au095817",
      "au097703",
      "au097406"
    )

  ets_ids <-
    c(
      "au002229",
      "au085279",
      "au093016",
      "au105905",
      "au107983",
      "au109844",
      "au120411",
      "au087953",
      "au105938",
      "au107604",
      "au109835",
      # "au109842", # change the day instead
      "au109858",
      "au109872",
      "au120487",
      "au120510",
      "au120392",
      "au120526",
      "au120409",
      "au120557",
      "nz039127"
    )

  tieds_ids <-
    c(
      "au086615", "au093294", "au105451",
      "au004037",
      "au019287",
      "au022612",
      "au033879",
      "au035738",
      "nz028887",
      "au070529",
      "au102459",
      "au026418",
      "au041373",
      "au044791",
      "au060760",
      "au060390",
      "au073132"
    )

  tesd_ids <-
    c(
      "au004315",
      "au009278",
      "au017326",
      "au027386",
      "au064259",
      "au082105",
      "au099101",
      "au005089",
      "au005359",
      "au020051",
      "au024135",
      "nz012551",
      "au063227",
      "au065411",
      "nz028507",
      "au074046",
      "au106285"
    )

  tse_ids <-
    c(
      "au012375",
      "au016741",
      "au016864",
      "au032283",
      "nz036813",
      "au107010",
      "au109686",
      "au115389",
      "nz012896",
      "au028728",
      "au046355",
      "au056810",
      "nz036001"
    )

  iteds_ids1 <-
    c(
      "au014272",
      "au028950",
      "au076491",
      "au089146",
      "au106863",
      "au111391",
      "au072595",
      "au076915",
      "au110749",
      "au111366",
      "au074499"
    )

  iteds_ids2 <-
    c(
      "au006589", # added after post-review on 02-Feb-2026
      "au013387",
      "au015116",
      "au089146",
      "au098899",
      "au072595"
      # "au074499"
    )

  iteds_ids3 <-
    c(
      "au013387",
      "au098899",
      "au006589",
      "au015116"
    )

  se_ids1 <-
    c(
      "au087898",
      "au087990",
      "au091472",
      "au087922",
      "au091448",
      "au087899",
      "au087909",
      "au091440",
      "au091466"
    )

  se_ids2 <-
    c(
      "au087928",
      "au091431",
      "au091470",
      "au087931"
    )

  tsi_ids <-
    c(
      "au028544",
      "au028565",
      "au028628",
      "au028559",
      "au028553",
      "au028567",
      "au028631",
      "au030021"
    )

  tedsi_ids <-
    c(
      "au029915",
      "au095538",
      "au097455",
      "au099237",
      "au100069",
      "au107654",
      "au060793",
      "au086616"
    )

  ied_ids <-
    c(
      "au094996",
      "au069867",
      "au073277",
      "au073342",
      "au083653",
      "au091984"
    )

  ise_ids <-
    c(
      "au014440", "au005223", "au027733", # added after post-review on 02-Feb-2026
      "au007863",
      "nz007210",
      "au026347",
      "au048169",
      "nz027068",
      "nz007165",
      "au026397"
    )

  ies_ids <-
    c(
      "au067057",
      "au012126",
      "au025010",
      "au112518",
      "au112646"
    )

  iedst_ids <-
    c(
      "au076253",
      "au073905",
      "au086595",
      "au087155",
      "au097832"
    )

  sed_ids <-
    c(
      "nz016184",
      "nz017441",
      "nz017965",
      "au100044"
    )

  its_ids <-
    c(
      "au050820",
      "au105893",
      "au106342",
      "au106474"
    )

  ist_ids <-
    c(
      "au094145",
      "au045503",
      "au066668"
    )

  et_ids <-
    c(
      "au027988",
      "nz032793",
      "nz036098"
    )

  tised_ids <-
    c(
      "au052670",
      "au093697",
      "au085055"
    )

  new_data <- raw_data |>
    # changes in pattern
    mutate(admdatetimeop = if_else(id %in% edsi_ids, NA, admdatetimeop)) |>
    mutate(depdatetime = if_else(id %in% esd_ids, NA, depdatetime)) |>
    mutate(tarrdatetime = if_else(id %in% edts_ids1, NA, tarrdatetime)) |>
    mutate(arrdatetime = if_else(id %in% edts_ids2, NA, arrdatetime)) |>
    mutate(depdatetime = if_else(id %in% edts_ids2, NA, depdatetime)) |>
    mutate(tarrdatetime = if_else(id %in% etds_ids, NA, tarrdatetime)) |>
    mutate(arrdatetime = if_else(id %in% tsed_ids, NA, arrdatetime)) |>
    mutate(depdatetime = if_else(id %in% tsed_ids, NA, depdatetime)) |>
    mutate(arrdatetime = if_else(id %in% ised_ids, NA, arrdatetime)) |>
    mutate(depdatetime = if_else(id %in% ised_ids, NA, depdatetime)) |>
    mutate(arrdatetime = if_else(id %in% ies_ids, NA, arrdatetime)) |>
    mutate(admdatetimeop = if_else(id %in% ieds_ids1, NA, admdatetimeop)) |>
    mutate(arrdatetime = if_else(id %in% ieds_ids2, NA, arrdatetime)) |>
    mutate(depdatetime = if_else(id %in% ieds_ids2, NA, depdatetime)) |>
    mutate(tarrdatetime = if_else(id %in% edst_ids, NA, tarrdatetime)) |>
    mutate(tarrdatetime = if_else(id %in% ets_ids, NA, tarrdatetime)) |>
    mutate(tarrdatetime = if_else(id %in% tieds_ids, NA, tarrdatetime)) |>
    mutate(arrdatetime = if_else(id %in% tieds_ids, NA, arrdatetime)) |>
    mutate(depdatetime = if_else(id %in% tieds_ids, NA, depdatetime)) |>
    mutate(depdatetime = if_else(id %in% tesd_ids, NA, depdatetime)) |>
    mutate(arrdatetime = if_else(id %in% tse_ids, NA, arrdatetime)) |>
    mutate(admdatetimeop = if_else(id %in% iteds_ids1, NA, admdatetimeop)) |>
    mutate(tarrdatetime = if_else(id %in% iteds_ids2, NA, tarrdatetime)) |>
    mutate(arrdatetime = if_else(id %in% iteds_ids3, NA, arrdatetime)) |>
    mutate(depdatetime = if_else(id %in% iteds_ids3, NA, depdatetime)) |>
    mutate(arrdatetime = if_else(id %in% se_ids1, NA, arrdatetime)) |>
    mutate(sdatetime = if_else(id %in% se_ids2, NA, sdatetime)) |>
    mutate(admdatetimeop = if_else(id %in% tsi_ids, NA, admdatetimeop)) |>
    mutate(admdatetimeop = if_else(id %in% tedsi_ids, NA, admdatetimeop)) |>
    mutate(arrdatetime = if_else(id %in% ied_ids, NA, arrdatetime)) |>
    mutate(depdatetime = if_else(id %in% ied_ids, NA, depdatetime)) |>
    mutate(arrdatetime = if_else(id %in% ise_ids, NA, arrdatetime)) |>
    mutate(tarrdatetime = if_else(id %in% iedst_ids, NA, tarrdatetime)) |>
    mutate(arrdatetime = if_else(id %in% iedst_ids, NA, arrdatetime)) |>
    mutate(depdatetime = if_else(id %in% iedst_ids, NA, depdatetime)) |>
    mutate(sdatetime = if_else(id %in% sed_ids, NA, sdatetime)) |>
    mutate(tarrdatetime = if_else(id %in% its_ids, NA, tarrdatetime)) |>
    mutate(tarrdatetime = if_else(id %in% ist_ids, NA, tarrdatetime)) |>
    mutate(tarrdatetime = if_else(id %in% et_ids, NA, tarrdatetime)) |>
    mutate(tarrdatetime = if_else(id %in% tised_ids, NA, tarrdatetime)) |>
    mutate(arrdatetime = if_else(id %in% tised_ids, NA, arrdatetime)) |>
    mutate(depdatetime = if_else(id %in% tised_ids, NA, depdatetime)) |>
    # bespoke changes
    # ESD
    mutate(gdate = if_else(id == "au003909", update(gdate, month = day(gdate), day = month(gdate)), gdate)) |>
    mutate(wdisch = if_else(id == "au048687", NA, wdisch)) |>
    mutate(sdatetime = if_else(id %in% c("au046593", "au113343", "au011450"), NA, sdatetime)) |>
    # EDTS
    mutate(arrdatetime = if_else(id == "au060435", update(arrdatetime, month = 5), arrdatetime)) |>
    mutate(depdatetime = if_else(id == "au060435", update(depdatetime, month = 5), depdatetime)) |>
    mutate(arrdatetime = if_else(id == "au099497", update(arrdatetime, month = 12), arrdatetime)) |>
    mutate(depdatetime = if_else(id == "au099497", update(depdatetime, month = 12), depdatetime)) |>
    mutate(arrdatetime = if_else(id == "au025426", update(arrdatetime, month = 01, year = 2019), arrdatetime)) |>
    mutate(depdatetime = if_else(id == "au025426", update(depdatetime, month = 01, year = 2019), depdatetime)) |>
    mutate(gdate = if_else(id == "au092740", NA, gdate)) |>
    # ETDS
    mutate(wdisch = if_else(id == "au061421", NA, wdisch)) |>
    # ISED
    mutate(gdate = if_else(id == "au099093", NA, gdate)) |>
    # IEDS
    mutate(gdate = if_else(id == "au037549", NA, gdate)) |>
    # EDST
    mutate(date120 = if_else(id == "au045859", NA, date120)) |>
    mutate(date120 = if_else(id == "nz024585", NA, date120)) |>
    # ETS
    mutate(arrdatetime = if_else(id == "au102134", update(arrdatetime, month = 3), arrdatetime)) |>
    mutate(arrdatetime = if_else(id == "au113320", update(arrdatetime, month = 11), arrdatetime)) |>
    mutate(arrdatetime = if_else(id == "au109842", update(arrdatetime, day = 10), arrdatetime)) |>
    mutate(arrdatetime = if_else(id == "au109578", NA, arrdatetime)) |>
    # TIEDS
    # mutate(tarrdatetime = if_else(id %in% c("au086615", "au093294", "au105451"), NA, tarrdatetime)) |> # become included in `tieds_ids`
    mutate(admdatetimeop = if_else(id %in% c("au061961", "au076160", "nz031172", "nz030810", "nz003017"), NA, admdatetimeop)) |>
    # TESD
    mutate(arrdatetime = if_else(id == "au027386", NA, arrdatetime)) |>
    mutate(sdatetime = if_else(id %in% c("au064274", "nz035428", "au074046"), NA, sdatetime)) |>
    # TSE
    mutate(arrdatetime = if_else(id == "au105099", update(arrdatetime, month = 4), arrdatetime)) |>
    mutate(arrdatetime = if_else(id == "au106385", update(arrdatetime, month = 5), arrdatetime)) |>
    # SE
    mutate(arrdatetime = if_else(id == "au091468", update(arrdatetime, day = 16), arrdatetime)) |>
    mutate(gdate = if_else(id %in% c("au087898", "au087899"), NA, gdate)) |>
    mutate(wdisch = if_else(id == "au087922", NA, wdisch)) |>
    # TEDSI
    mutate(tarrdatetime = if_else(id == "au107654", NA, tarrdatetime)) |>
    mutate(gdate = if_else(id == "au086616", NA, gdate)) |>
    # IED
    mutate(admdatetimeop = if_else(id == "au061377", update(admdatetimeop, month = 7), admdatetimeop)) |>
    # IEDST
    mutate(admdatetimeop = if_else(id == "au097832", NA, admdatetimeop)) |>
    # SED
    mutate(gdate = if_else(id %in% c("nz016184", "nz017441"), NA, gdate)) |>
    # TESI
    mutate(admdatetimeop = if_else(id %in% c("au098844", "au090376"), NA, admdatetimeop)) |>
    # TIES
    mutate(tarrdatetime = if_else(id %in% c("au025566", "au090985"), NA, tarrdatetime)) |>
    mutate(arrdatetime = if_else(id %in% c("au025566", "au090985"), NA, arrdatetime)) |>
    # ETD
    mutate(tarrdatetime = if_else(id %in% c("au057349", "au048372"), NA, tarrdatetime)) |>
    # TSD
    mutate(depdatetime = if_else(id %in% c("au070646", "au017542"), NA, depdatetime)) |>
    # ISTED
    mutate(tarrdatetime = if_else(id %in% c("au070879", "au025341"), NA, tarrdatetime)) |>
    mutate(arrdatetime = if_else(id %in% c("au070879", "au025341"), NA, arrdatetime)) |>
    mutate(depdatetime = if_else(id %in% c("au070879", "au025341"), NA, depdatetime)) |>
    # EDTIS
    mutate(tarrdatetime = if_else(id %in% c("nz017275", "au061182"), NA, tarrdatetime)) |>
    mutate(arrdatetime = if_else(id %in% c("nz017275", "au061182"), NA, arrdatetime)) |>
    mutate(depdatetime = if_else(id %in% c("nz017275", "au061182"), NA, depdatetime)) |>
    # DES
    mutate(depdatetime = if_else(id == "au017488", NA, depdatetime)) |>
    # ITDS
    mutate(tarrdatetime = if_else(id == "au096078", NA, tarrdatetime)) |>
    mutate(depdatetime = if_else(id == "au096078", NA, depdatetime)) |>
    # EDT
    mutate(tarrdatetime = if_else(id == "au074055", NA, tarrdatetime)) |>
    # STED
    mutate(tarrdatetime = if_else(id == "nz031611", NA, tarrdatetime)) |>
    mutate(sdatetime = if_else(id == "nz031611", NA, sdatetime)) |>
    mutate(gdate = if_else(id == "nz031611", NA, gdate)) |>
    # DTS
    mutate(depdatetime = if_else(id == "au056230", NA, depdatetime)) |>
    # EST
    mutate(tarrdatetime = if_else(id == "au091428", NA, tarrdatetime)) |>
    mutate(gdate = if_else(id == "au091428", NA, gdate)) |>
    # ITES
    mutate(admdatetimeop = if_else(id == "au095291", NA, admdatetimeop)) |>
    # EITS
    mutate(tarrdatetime = if_else(id == "au054581", NA, tarrdatetime)) |>
    mutate(arrdatetime = if_else(id == "au054581", NA, arrdatetime)) |>
    # EISD
    mutate(arrdatetime = if_else(id == "nz007554", NA, arrdatetime)) |>
    mutate(depdatetime = if_else(id == "nz007554", NA, depdatetime)) |>
    # EDIST
    mutate(tarrdatetime = if_else(id == "au115200", NA, tarrdatetime)) |>
    mutate(arrdatetime = if_else(id == "au115200", NA, arrdatetime)) |>
    mutate(depdatetime = if_else(id == "au115200", NA, depdatetime)) |>
    # ITSED
    mutate(tarrdatetime = if_else(id == "au074356", NA, tarrdatetime)) |>
    mutate(arrdatetime = if_else(id == "au074356", NA, arrdatetime)) |>
    mutate(depdatetime = if_else(id == "au074356", NA, depdatetime)) |>
    # ESI
    mutate(admdatetimeop = if_else(id == "au067463", NA, admdatetimeop)) |>
    # ET (identified after post-review)
    mutate(tarrdatetime = if_else(id == "au118462", NA, tarrdatetime)) |>
    # DIST (identified after post-review)
    mutate(depdatetime = if_else(id == "au100142", NA, depdatetime)) |>
    mutate(tarrdatetime = if_else(id == "au100142", NA, tarrdatetime))

  return(new_data)
}

# hdisch_review <- function(raw_data) {
#
#   invalid_hdisch_ids <- c(
#     "nz016165",
#   )
#
#   invalid_wdisch_ids <- c(
#     "nz016165",
#   )
#
#
#   new_data <- raw_data |>
#     mutate(hdisch = if_else(id == "au020235", update(hdisch, year = 2018), hdisch)) |>
#     mutate(hdisch = if_else(id == "au008201", update(hdisch, month = 11, day = 19), hdisch)) |>
#
#
#
#   return(new_data)
#
# }
