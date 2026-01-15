# Rate Study
-- AKC Rate study
USE ProductionOfflineReportingViewsGB;
with table_1 as (
select 
[ObjectID],

[FirstName], 
[LastName], 
[MedicalRecordNo] as MRN,
[NationalHealthServiceNo] as NHS_no,
[DateOfBirth_Local] as DOB,
[Sex],
[Ethnicity],
[DateOfDeath_Local] as DOD,
PrimaryRenalDiagnosis, 

DiabeticStatus, 
IsActive, 
InactiveDate_Local,
PrimaryRenalModalityStartDate_Local,
PrimaryRenalModality,

progress_note.Date_Local as 'first_progress_note_date',
progress_note.ProgressNoteType as 'first_progress_note_type',
adequacy.LabTestDate_Local as 'first_eGFR_date',
adequacy.eGFRNum as 'first_eGFR'
from RenalPatient

outer apply (
       select top 1 PatientObjectId, Date_Local,[ProgressNoteType]  from PatientProgressNotes
       where RenalPatient.ObjectID = PatientProgressNotes.PatientObjectID
       order by PatientProgressNotes.Date_Local asc
) progress_note

cross apply (
       select top 1 eGFRNum, PatientObjectId, LabTestDate_Local  from PatientLabPanelRenalAdequacy
       where RenalPatient.ObjectID = PatientLabPanelRenalAdequacy.PatientObjectID
	   and eGFRNum is not null
       order by PatientLabPanelRenalAdequacy.LabTestDate_Local asc
) adequacy
),

table_3 as (

select
[ObjectID],
[FirstName], 
[LastName], 
MRN,
NHS_no,
DOB,
[Sex],
[Ethnicity],
DOD,
PrimaryRenalDiagnosis, 
DiabeticStatus, 
IsActive, 
InactiveDate_Local,
PrimaryRenalModalityStartDate_Local,
PrimaryRenalModality,
first_progress_note_date,
first_progress_note_type,
egfr_at_progress_note.LabTestDate_Local as 'close_to_progress_note_date',
egfr_at_progress_note.eGFRNum as 'close_to_progress_eGFR',
first_eGFR_date,
first_eGFR,
first_filter.LabTestDate_Local as 'index_1_date',
first_filter.eGFRNum as 'index_1_eGFR',
trial_date_1 as 'test_date_1',
trial_1 as 'test_eGFR_1'
from table_1

outer apply (
       select top 1 eGFRNum, PatientObjectId, LabTestDate_Local  from PatientLabPanelRenalAdequacy
       where table_1.ObjectID = PatientLabPanelRenalAdequacy.PatientObjectID
	   and LabTestDate_Local >= first_progress_note_date
	   and PatientLabPanelRenalAdequacy.eGFRNum is not null
       order by PatientLabPanelRenalAdequacy.LabTestDate_Local asc
) egfr_at_progress_note

outer apply (
       select PatientObjectId, eGFRNum, LabTestDate_Local,
	   LEAD(eGFRNum) over (partition by PatientObjectId order by LabTestDate_Local) as trial_1,
	   LEAD(LabTestDate_Local) over (partition by PatientObjectId order by LabTestDate_Local) as trial_date_1
	   from PatientLabPanelRenalAdequacy
       where table_1.ObjectID = PatientLabPanelRenalAdequacy.PatientObjectID
	   and PatientLabPanelRenalAdequacy.LabTestDate_Local >= table_1.first_eGFR_date
	   and PatientLabPanelRenalAdequacy.eGFRNum is not null
	   -- PatientLabPanelRenalAdequacy.eGFRNum between 20 and 30
       --order by PatientLabPanelRenalAdequacy.LabTestDate_Local asc
) first_filter
), 

table_4 as (
select 
[ObjectID],
[FirstName], 
[LastName], 
MRN,
NHS_no,
DOB,
[Sex],
[Ethnicity],
DOD,
PrimaryRenalDiagnosis, 
DiabeticStatus, 
IsActive, 
InactiveDate_Local,
PrimaryRenalModalityStartDate_Local,
PrimaryRenalModality,
first_eGFR_date,
first_eGFR,
first_progress_note_date,
first_progress_note_type,
close_to_progress_note_date,
close_to_progress_eGFR,
index_1_date,
index_1_eGFR,
test_date_1,
test_eGFR_1,
second_filter.LabTestDate_Local as 'index_2_date',
second_filter.eGFRNum as 'index_2_eGFR',
trial_date as 'test_date_2',
trial as 'test_eGFR_2'

from table_3

outer apply (
       select PatientObjectId, eGFRNum, LabTestDate_Local, 
	   LEAD(eGFRNum) over (partition by PatientObjectId order by LabTestDate_Local) as trial,
	   LEAD(LabTestDate_Local) over (partition by PatientObjectId order by LabTestDate_Local) as trial_date
	   from PatientLabPanelRenalAdequacy
       where table_3.ObjectID = PatientLabPanelRenalAdequacy.PatientObjectID
	   and PatientLabPanelRenalAdequacy.LabTestDate_Local > index_1_date
	   and PatientLabPanelRenalAdequacy.eGFRNum is not null
	   --order by PatientLabPanelRenalAdequacy.LabTestDate_Local asc
) second_filter
),

table_6 as (
select 
[ObjectID],
[FirstName], 
[LastName], 
MRN,
NHS_no,
DOB,
[Sex],
[Ethnicity],
DOD,
PrimaryRenalDiagnosis, 
DiabeticStatus, 
IsActive, 
InactiveDate_Local,
PrimaryRenalModalityStartDate_Local,
PrimaryRenalModality,
first_eGFR_date,
first_eGFR,
first_progress_note_date,
first_progress_note_type,
close_to_progress_note_date,
close_to_progress_eGFR,
index_1_date,
index_1_eGFR,
test_date_1,
test_eGFR_1,
index_2_date,
index_2_eGFR,
test_date_2,
test_eGFR_2,
ROW_NUMBER() OVER (PARTITION BY [ObjectID] ORDER BY index_1_date) AS RN_1,
ROW_NUMBER() OVER (PARTITION BY [ObjectID] ORDER BY index_2_date) AS RN_2
from table_4
where (index_2_eGFR < 20 and test_eGFR_2 < 20)
and (index_1_eGFR between 20 and 30 and test_eGFR_1 < 30)
),

table_7 as(
select
[ObjectID],
[FirstName], 
[LastName], 
MRN,
NHS_no,
DOB,
[Sex],
[Ethnicity],
DOD,
PrimaryRenalDiagnosis, 
DiabeticStatus, 
IsActive, 
InactiveDate_Local,
modal.Modality,
modal.StartDate_Local as Modality_StartDate_Local,

modal_esrf.Modality as first_ESRF_modality,
modal_esrf.StartDate_Local as date_ESRF,

latest.eGFRNum as eGFR_latest_1,
latest.LabTestDate_Local as date_eGFR_latest_1,

PrimaryRenalModalityStartDate_Local,
PrimaryRenalModality,
CONVERT(VARCHAR, first_eGFR_date, 103) as 'first_eGFR_date',
first_eGFR,
first_progress_note_date as 'first_progress_note_date',
first_progress_note_type,
CONVERT(VARCHAR, close_to_progress_note_date,103) as 'date_eGFR_at_first_progress_note',
close_to_progress_eGFR as 'eGFR_at_first_progress_note',
--CONVERT(VARCHAR, index_1_date,103) as 'index_date_1',
index_1_date as index_date_1,
index_1_eGFR as 'index_eGFR_1',
CONVERT(VARCHAR, test_date_1,103) as 'test_date_1',
test_eGFR_1,
--CONVERT(VARCHAR, index_2_date,103) as 'index_date_2',
index_2_date as index_date_2,
index_2_eGFR as 'index_eGFR_2',
CONVERT(VARCHAR, test_date_2,103) as 'test_date_2',
test_eGFR_2
--,RN_1
--,RN_2
from table_6

outer apply(
select top 1 [PatientObjectID], [Modality], [StartDate_Local] from [dbo].[PatientRenalModality]
where PatientRenalModality.PatientObjectID = table_6.ObjectID
order by StartDate_Local desc
) modal

outer apply(
select top 1 [PatientObjectID], [Modality], [StartDate_Local] from [dbo].[PatientRenalModality]
where PatientRenalModality.PatientObjectID = table_6.ObjectID
and ([PatientRenalModality].Modality ='HD' or [PatientRenalModality].Modality ='PD' or [PatientRenalModality].Modality ='Transplant')
order by StartDate_Local asc
) modal_esrf

outer apply (
       select top 1 eGFRNum, PatientObjectId, LabTestDate_Local  from PatientLabPanelRenalAdequacy
       where table_6.ObjectID = PatientLabPanelRenalAdequacy.PatientObjectID
	   and PatientLabPanelRenalAdequacy.eGFRNum is not null
       order by PatientLabPanelRenalAdequacy.LabTestDate_Local desc
) latest



where (RN_2 = 1 and RN_1=1)
and first_eGFR > 20
and close_to_progress_eGFR > 20
),

table_8 as (

select 
[ObjectID],
[FirstName], 
[LastName], 
MRN,
NHS_no,
DOB,
[Sex],
[Ethnicity],
DOD,

PrimaryRenalDiagnosis, 
DiabeticStatus, 
IsActive, 
InactiveDate_Local,

table_7.Modality,
Modality_StartDate_Local,

Modality_AKC.StartDate_Local as Modality_AKC,
Modality_LC.StartDate_Local as Modality_LC,
Modality_Pre.StartDate_Local as Modality_Pre,
Modality_CM.StartDate_Local as Modality_CM,
first_AC.Date_Local as first_access_clinic,


first_ESRF_modality,
date_ESRF,
ESRF.LabTestDate_Local as date_ESRF_eGFR,
ESRF.eGFRNum as eGFR_at_ESRF,

first_eGFR_date,
first_eGFR,
first_progress_note_date,
first_progress_note_type,
date_eGFR_at_first_progress_note,
eGFR_at_first_progress_note,
index_date_1,
index_eGFR_1,
test_date_1,
test_eGFR_1,
index_date_2,
index_eGFR_2,
test_date_2,
test_eGFR_2,

index_2.LabTestDate_Local as index_date_2_1_year,
index_2.eGFRNum as index_eGFR_2_1_year,

index_2_prior.LabTestDate_Local as index_date_1_year_prior,
index_2_prior.eGFRNum as index_eGFR_1_year_prior,

eGFR_latest_1,
date_eGFR_latest_1,

lowest.LabTestDate_Local as date_lowest_eGFR,
lowest.eGFRNum as eGFR_lowest,

med_1.DrugName as DrugName_1,
med_2.DrugName as DrugName_2,
med_3.DrugName as DrugName_3,
med_4.DrugName as DrugName_4,

med_1.[StartDate_Local] as DrugName_1_startdate,
med_2.[StartDate_Local] as DrugName_2_startdate,
med_3.[StartDate_Local] as DrugName_3_startdate,
med_4.[StartDate_Local] as DrugName_4_startdate,

med_1_stop.[StopDate_Local] as DrugName_1_stopdate,
med_2_stop.[StopDate_Local] as DrugName_2_stopdate,
med_3_stop.[StopDate_Local] as DrugName_3_stopdate,
med_4_stop.[StopDate_Local] as DrugName_4_stopdate,

med_1_check.[StartDate_Local] as DrugName_1_check_date,
med_2_check.[StartDate_Local] as DrugName_2_check_date,
med_3_check.[StartDate_Local] as DrugName_3_check_date,
med_4_check.[StartDate_Local] as DrugName_4_check_date




--,RN_1
--,RN_2
from table_7

outer apply (
       select top 1 eGFRNum, PatientObjectId, LabTestDate_Local  from PatientLabPanelRenalAdequacy
       where table_7.ObjectID = PatientLabPanelRenalAdequacy.PatientObjectID
	   and PatientLabPanelRenalAdequacy.LabTestDate_Local <= date_ESRF
	   and PatientLabPanelRenalAdequacy.eGFRNum is not null
       order by PatientLabPanelRenalAdequacy.LabTestDate_Local desc
) ESRF

outer apply (
       select top 1 eGFRNum, PatientObjectId, LabTestDate_Local  from PatientLabPanelRenalAdequacy
       where table_7.ObjectID = PatientLabPanelRenalAdequacy.PatientObjectID
	   and PatientLabPanelRenalAdequacy.LabTestDate_Local >= dateadd(year,1,index_date_2)
	   and PatientLabPanelRenalAdequacy.eGFRNum is not null
       order by PatientLabPanelRenalAdequacy.LabTestDate_Local asc
) index_2

outer apply (
       select top 1 eGFRNum, PatientObjectId, LabTestDate_Local  from PatientLabPanelRenalAdequacy
       where table_7.ObjectID = PatientLabPanelRenalAdequacy.PatientObjectID
	   and PatientLabPanelRenalAdequacy.LabTestDate_Local <= dateadd(year,-1,index_date_2)
	   and PatientLabPanelRenalAdequacy.eGFRNum is not null
       order by PatientLabPanelRenalAdequacy.LabTestDate_Local desc
) index_2_prior

outer apply (
       select top 1 eGFRNum, PatientObjectId, LabTestDate_Local  from PatientLabPanelRenalAdequacy
       where table_7.ObjectID = PatientLabPanelRenalAdequacy.PatientObjectID
	   and PatientLabPanelRenalAdequacy.eGFRNum is not null
	   order by PatientLabPanelRenalAdequacy.eGFRNum asc
) lowest

outer apply (
select top 1 [PatientObjectID], [StartDate_Local],[StopDate_Local], DrugName from PatientMedicationOrders
where table_7.ObjectID = PatientMedicationOrders.PatientObjectID
and DrugName like '%gliflozin%' 
order by PatientMedicationOrders.[StartDate_Local] asc
) med_1

outer apply (
select top 1 [PatientObjectID], [StartDate_Local],[StopDate_Local], DrugName from PatientMedicationOrders
where table_7.ObjectID = PatientMedicationOrders.PatientObjectID
and DrugName like '%pril%' 
order by PatientMedicationOrders.[StartDate_Local] asc
) med_2

outer apply (
select top 1 [PatientObjectID], [StartDate_Local],[StopDate_Local], DrugName from PatientMedicationOrders
where table_7.ObjectID = PatientMedicationOrders.PatientObjectID
and DrugName like '%sartan%' 
order by PatientMedicationOrders.[StartDate_Local] asc
) med_3

outer apply (
select top 1 [PatientObjectID], [StartDate_Local],[StopDate_Local], DrugName from PatientMedicationOrders
where table_7.ObjectID = PatientMedicationOrders.PatientObjectID
and DrugName like 'Diltiazem%' 
order by PatientMedicationOrders.[StartDate_Local] asc
) med_4

outer apply (
select top 1 [PatientObjectID], [StartDate_Local],[StopDate_Local], DrugName from PatientMedicationOrders
where table_7.ObjectID = PatientMedicationOrders.PatientObjectID
and DrugName like '%gliflozin%' 
order by PatientMedicationOrders.[StopDate_Local] desc
) med_1_stop

outer apply (
select top 1 [PatientObjectID], [StartDate_Local],[StopDate_Local], DrugName from PatientMedicationOrders
where table_7.ObjectID = PatientMedicationOrders.PatientObjectID
and DrugName like '%pril%' 
order by PatientMedicationOrders.[StopDate_Local] desc
) med_2_stop

outer apply (
select top 1 [PatientObjectID], [StartDate_Local],[StopDate_Local], DrugName from PatientMedicationOrders
where table_7.ObjectID = PatientMedicationOrders.PatientObjectID
and DrugName like '%sartan%' 
order by PatientMedicationOrders.[StopDate_Local] desc
) med_3_stop

outer apply (
select top 1 [PatientObjectID], [StartDate_Local],[StopDate_Local], DrugName from PatientMedicationOrders
where table_7.ObjectID = PatientMedicationOrders.PatientObjectID
and DrugName like 'Diltiazem%' 
order by PatientMedicationOrders.[StopDate_Local] desc
) med_4_stop

outer apply (
select top 1 [PatientObjectID], [StartDate_Local],[StopDate_Local], DrugName from PatientMedicationOrders
where table_7.ObjectID = PatientMedicationOrders.PatientObjectID
and DrugName like '%gliflozin%' 
order by PatientMedicationOrders.[StartDate_Local] desc
) med_1_check

outer apply (
select top 1 [PatientObjectID], [StartDate_Local],[StopDate_Local], DrugName from PatientMedicationOrders
where table_7.ObjectID = PatientMedicationOrders.PatientObjectID
and DrugName like '%pril%' 
order by PatientMedicationOrders.[StartDate_Local] desc
) med_2_check

outer apply (
select top 1 [PatientObjectID], [StartDate_Local],[StopDate_Local], DrugName from PatientMedicationOrders
where table_7.ObjectID = PatientMedicationOrders.PatientObjectID
and DrugName like '%sartan%' 
order by PatientMedicationOrders.[StartDate_Local] desc
) med_3_check

outer apply (
select top 1 [PatientObjectID], [StartDate_Local],[StopDate_Local], DrugName from PatientMedicationOrders
where table_7.ObjectID = PatientMedicationOrders.PatientObjectID
and DrugName like 'Diltiazem%' 
order by PatientMedicationOrders.[StartDate_Local] desc
) med_4_check

outer apply (
select top 1 [PatientObjectID], [Modality], [StartDate_Local] from PatientRenalModality
where table_7.ObjectID = PatientRenalModality.PatientObjectID 
and PatientRenalModality.Modality like 'AKC'
order by StartDate_Local asc
) Modality_AKC

outer apply (
select top 1 [PatientObjectID], [Modality], [StartDate_Local] from PatientRenalModality
where table_7.ObjectID = PatientRenalModality.PatientObjectID 
and PatientRenalModality.Modality like 'Low Clearance'
order by StartDate_Local asc
) Modality_LC

outer apply (
select top 1 [PatientObjectID], [Modality], [StartDate_Local] from PatientRenalModality
where table_7.ObjectID = PatientRenalModality.PatientObjectID 
and PatientRenalModality.Modality like 'Predialysis'
order by StartDate_Local asc
) Modality_Pre

outer apply (
select top 1 [PatientObjectID], [Modality], [StartDate_Local] from PatientRenalModality
where table_7.ObjectID = PatientRenalModality.PatientObjectID 
and PatientRenalModality.Modality like 'Conservative Management'
order by StartDate_Local asc
) Modality_CM

outer apply (
select top 1 [PatientObjectID], ProgressNoteType, Date_Local from PatientProgressNotes
where table_7.ObjectID = PatientProgressNotes.PatientObjectID 
and PatientProgressNotes.ProgressNoteType like '%Access%'
order by Date_Local asc
) first_AC

),

table_9 as (
select 
[ObjectID],
[FirstName], 
[LastName], 
MRN,
NHS_no,
DOB,
[Sex],
[Ethnicity],
DOD,

PrimaryRenalDiagnosis, 
DiabeticStatus, 
IsActive, 
InactiveDate_Local,

Modality,
Modality_StartDate_Local,

first_ESRF_modality,
date_ESRF,
date_ESRF_eGFR,
eGFR_at_ESRF,

first_eGFR_date,
first_eGFR,
first_progress_note_date,
first_progress_note_type,
date_eGFR_at_first_progress_note,
eGFR_at_first_progress_note,
index_date_1,
index_eGFR_1,
test_date_1,
test_eGFR_1,
index_date_2,
index_eGFR_2,
test_date_2,
test_eGFR_2,

index_date_2_1_year,
index_eGFR_2_1_year,

index_date_1_year_prior,
index_eGFR_1_year_prior,


date_eGFR_latest_1,
eGFR_latest_1,

date_lowest_eGFR,
eGFR_lowest,
case
when (datediff(day,index_date_2,index_date_1) = 0 or index_date_2 is null or index_date_1 is null) then null
else round(365*(index_eGFR_2-index_eGFR_1)/datediff(day,index_date_2,index_date_1), 1) 
end as rate_of_fall_1,

case
when (datediff(day,index_date_2_1_year,index_date_2) = 0 or index_date_2_1_year is null or index_date_2 is null or date_ESRF between 0 and index_date_2_1_year) then null
else round(365*(index_eGFR_2_1_year-index_eGFR_2)/datediff(day,index_date_2_1_year,index_date_2), 1)
end as rate_of_fall_2,

case
when date_ESRF is null and datediff(day,date_eGFR_latest_1,index_date_2) = 0 then null 
when date_ESRF is null then round(365*(eGFR_latest_1-index_eGFR_2)/datediff(day,date_eGFR_latest_1,index_date_2) ,1)
when (datediff(day,date_ESRF_eGFR,index_date_2) = 0 or date_ESRF_eGFR is null or index_date_2 is null) then null
else round(365*(eGFR_at_ESRF-index_eGFR_2)/datediff(day,date_ESRF_eGFR,index_date_2) ,1)
end as rate_of_fall_3,

case
when index_date_1_year_prior is null or index_date_2 is null or datediff(day,index_date_2,index_date_1_year_prior) = 0 then null
else round(365*(index_eGFR_2-index_eGFR_1_year_prior)/datediff(day,index_date_2,index_date_1_year_prior), 1)
end as rate_of_fall_4,

uACR.[LabTestDate_Local] as date_uACR_index_1,
uACR.[AlbuminCreatinineRatioNum] as uACR_index_1 ,
uPCR.[LabTestDate_Local] as date_uPCR_index_1,
uPCR.[ProteinCreatinineRatioNum] as uPCR_index_1 ,

uACR_max.[LabTestDate_Local] as date_uACR_highest,
uACR_max.[AlbuminCreatinineRatioNum] as uACR_highest,

uPCR_max.[LabTestDate_Local] as date_uPCR_highest,
uPCR_max.[ProteinCreatinineRatioNum] as uPCR_highest,

uACR_desc.LabTestDate_Local as uACR_date_desc,
uACR_desc.AlbuminCreatinineRatioNum as uACR_num_desc,

case
when abs(datediff(day,uACR_desc.LabTestDate_Local,index_date_2)) < abs(datediff(day,uPCR_desc.LabTestDate_Local,index_date_2)) or uPCR_desc.LabTestDate_Local is null then uACR_desc.LabTestDate_Local
else uPCR_desc.LabTestDate_Local
end as uACR_conversion_date,

case
when abs(datediff(day,uACR_desc.LabTestDate_Local,index_date_2)) < abs(datediff(day,uPCR_desc.LabTestDate_Local,index_date_2)) or uPCR_desc.LabTestDate_Local is null then uACR_desc.AlbuminCreatinineRatioNum
else 0.7 * uPCR_desc.ProteinCreatinineRatioNum
end as uACR_conversion_val,

case
when abs(datediff(day,uACR_desc.LabTestDate_Local,index_date_2)) < abs(datediff(day,uPCR_desc.LabTestDate_Local,index_date_2)) or uPCR_desc.LabTestDate_Local is null then 'uACR'
when uPCR_desc.LabTestDate_Local is not null then 'uPCR'
else null
end as uACR_or_uPCR,



uPCR_desc.LabTestDate_Local as uPCR_date_desc,
uPCR_desc.ProteinCreatinineRatioNum as uPCR_num_desc,

DrugName_1,
DrugName_2,
DrugName_3,
DrugName_4,

DrugName_1_startdate,
DrugName_2_startdate,
DrugName_3_startdate,
DrugName_4_startdate,

case
when datediff(day,DrugName_1_stopdate,DrugName_1_check_date) between 0 and 1 then null
else DrugName_1_stopdate
end as DrugName_1_stopdate,
case
when datediff(day,DrugName_2_stopdate,DrugName_2_check_date) between 0 and 1 then null
else DrugName_2_stopdate
end as DrugName_2_stopdate,
case
when datediff(day,DrugName_3_stopdate,DrugName_3_check_date) between 0 and 1 then null
else DrugName_3_stopdate
end as DrugName_3_stopdate,
case
when datediff(day,DrugName_4_stopdate,DrugName_4_check_date) between 0 and 1 then null
else DrugName_4_stopdate
end as DrugName_4_stopdate,

Modality_AKC as date_AKC_modality,
Modality_LC as date_Low_Clearance_modality,
Modality_Pre as date_Predialysis_modality,
Modality_CM as date_CM_modality,
first_access_clinic as date_Dx_Access_Clinic,

reason.[AdmitDate_Local],
reason.[ChiefComplaint] 
from table_8

outer apply (
select top 1 [PatientObjectID],[LabTestDate_Local],[AlbuminCreatinineRatioNum] from PatientLabPanelUrineChemistry
where table_8.ObjectID = PatientLabPanelUrineChemistry.PatientObjectID
and [AlbuminCreatinineRatioNum] is not null
and [LabTestDate_Local] <= index_date_1
order by [LabTestDate_Local] desc
) uACR

outer apply (
select top 1 [PatientObjectID],[LabTestDate_Local],[AlbuminCreatinineRatioNum] from PatientLabPanelUrineChemistry
where table_8.ObjectID = PatientLabPanelUrineChemistry.PatientObjectID
and [AlbuminCreatinineRatioNum] is not null
and [LabTestDate_Local] <= index_date_2
order by [LabTestDate_Local] desc
) uACR_desc

outer apply (
select top 1 [PatientObjectID],[LabTestDate_Local],[ProteinCreatinineRatioNum] from PatientLabPanelUrineChemistry
where table_8.ObjectID = PatientLabPanelUrineChemistry.PatientObjectID
and [ProteinCreatinineRatioNum] is not null
and [LabTestDate_Local] <= index_date_2
order by [LabTestDate_Local] desc
) uPCR_desc



outer apply (
select top 1 [PatientObjectID],[LabTestDate_Local],[ProteinCreatinineRatioNum] from PatientLabPanelUrineChemistry
where table_8.ObjectID = PatientLabPanelUrineChemistry.PatientObjectID
and [ProteinCreatinineRatioNum] is not null
and [LabTestDate_Local] <= index_date_1
order by [LabTestDate_Local] desc
) uPCR

outer apply (
select top 1 [PatientObjectID],[LabTestDate_Local],[AlbuminCreatinineRatioNum] from PatientLabPanelUrineChemistry
where table_8.ObjectID = PatientLabPanelUrineChemistry.PatientObjectID
and [AlbuminCreatinineRatioNum] is not null
order by [AlbuminCreatinineRatioNum] desc
) uACR_max

outer apply (
select top 1 [PatientObjectID],[LabTestDate_Local],[ProteinCreatinineRatioNum] from PatientLabPanelUrineChemistry
where table_8.ObjectID = PatientLabPanelUrineChemistry.PatientObjectID
and [ProteinCreatinineRatioNum] is not null
order by [ProteinCreatinineRatioNum] desc
) uPCR_max

outer apply (
select top 1 [PatientObjectID],[AdmitDate_Local],[ChiefComplaint] from [dbo].[PatientEpisodeHistory]
where table_8.ObjectID = [PatientEpisodeHistory].[PatientObjectID]
and ([ChiefComplaint] like '%RCF%' or [ChiefComplaint] like '%BCF%' or [ChiefComplaint] like '%AVF%' or [ChiefComplaint] like '%Fistula%' or [ChiefComplaint] like '%line%' or [ChiefComplaint] like '%catheter%' or [ChiefComplaint] like '%tesio%' or [ChiefComplaint] like '%Access%')
order by [PatientEpisodeHistory].[AdmitDate_Local] asc
) reason
),

table_fin as (

select 
[FirstName], 
[LastName], 
DOB,
[Sex],
case
when sex like 'Female' then 0
else 1
end as gender_KFRE,
[Ethnicity],
DOD,

case
when DOD is null then 0
else 1
end as deceased_or_not,

IsActive, 
InactiveDate_Local,
Modality as Latest_Modality,
Modality_StartDate_Local as Date_Latest_Modality,

PrimaryRenalDiagnosis, 
DiabeticStatus, 

date_ESRF,
date_ESRF_eGFR,
eGFR_at_ESRF,

first_ESRF_modality,

first_eGFR_date as date_1st_eGFR,
first_eGFR as eGFR_1st,
first_progress_note_date as date_1st_prog_note,
first_progress_note_type,
date_eGFR_at_first_progress_note as date_1st_prog_note_eGFR,
eGFR_at_first_progress_note as eGFR_1st_prog_note,
index_date_1 as date_stage_4,
index_eGFR_1 as egFR_stage_4,
test_date_1,
test_eGFR_1,
index_date_2 as date_index,
index_eGFR_2 as eGFR_index,
test_date_2 as date_post_index,
test_eGFR_2 as eGFR_post_index,

case
when index_date_2_1_year > date_ESRF then null
else index_date_2_1_year
end as index_2_1_year_date,

case
when index_date_2_1_year > date_ESRF then null
else index_eGFR_2_1_year
end as index_2_1_year_eGFR,

index_date_1_year_prior,
index_eGFR_1_year_prior,

case 
when date_ESRF is null then date_eGFR_latest_1
else null
end as date_eGFR_latest,
case 
when date_ESRF is null then eGFR_latest_1
else null
end as eGFR_latest,

case
when date_lowest_eGFR > date_ESRF then null
else date_lowest_eGFR
end as date_lowest_eGFR,

case
when date_lowest_eGFR > date_ESRF then null
else eGFR_lowest
end as eGFR_lowest,

uACR_conversion_date,
uACR_conversion_val,

rate_of_fall_1 as fall_1_4_index,
rate_of_fall_2 as fall_2_index_1yr,
rate_of_fall_3 as fall_3_index_last,
rate_of_fall_4 as fall_4_index_1_year_prior,
rate_of_fall_1-rate_of_fall_2 as delta_1_2,
rate_of_fall_1-rate_of_fall_3 as delta_1_3,
rate_of_fall_2-rate_of_fall_3 as delta_2_3,
rate_of_fall_2-rate_of_fall_4 as delta_2_4,

case
when date_ESRF is null then null
else datediff(month,index_date_2,date_ESRF)
end as time_to_ESRF,

case
when index_date_1 is null then null
else datediff(day,index_date_1,index_date_2)
end as index_1_to_index_2_time,

date_AKC_modality,
date_Low_Clearance_modality,
date_Predialysis_modality,
date_CM_modality,
date_Dx_Access_Clinic,

case
when date_AKC_modality is null and date_Low_Clearance_modality is null and date_Predialysis_modality is null then null
when (date_AKC_modality is null and date_Low_Clearance_modality is null) or (date_Low_Clearance_modality is null and date_Predialysis_modality < date_AKC_modality) then 'Predialysis'
when (date_Low_Clearance_modality is null and date_Predialysis_modality is null) or (date_Predialysis_modality is null and date_AKC_modality < date_Low_Clearance_modality) then 'AKC'
when (date_AKC_modality is null and date_Predialysis_modality is null) or (date_AKC_modality is null and date_Low_Clearance_modality < date_Predialysis_modality) then 'Low Clearance'
when (date_AKC_modality < date_Low_Clearance_modality and date_AKC_modality < date_Predialysis_modality) then 'AKC'
when (date_Low_Clearance_modality < date_AKC_modality)  and (date_Low_Clearance_modality < date_Predialysis_modality) then 'Low Clearance'
else 'Predialysis'
end as first_pre_RRT_modality,


case
when date_AKC_modality is null and date_Low_Clearance_modality is null and date_Predialysis_modality is null then null
when (date_AKC_modality is null and date_Low_Clearance_modality is null) or (date_Low_Clearance_modality is null and date_Predialysis_modality < date_AKC_modality) then date_Predialysis_modality
when (date_Low_Clearance_modality is null and date_Predialysis_modality is null) or (date_Predialysis_modality is null and date_AKC_modality < date_Low_Clearance_modality) then date_AKC_modality
when (date_AKC_modality is null and date_Predialysis_modality is null) or (date_AKC_modality is null and date_Low_Clearance_modality < date_Predialysis_modality) then date_Low_Clearance_modality
when (date_AKC_modality < date_Low_Clearance_modality and date_AKC_modality < date_Predialysis_modality) then date_AKC_modality
when (date_Low_Clearance_modality < date_AKC_modality)  and (date_Low_Clearance_modality < date_Predialysis_modality) then date_Low_Clearance_modality
else date_Predialysis_modality
end as first_pre_RRT_modality_date,

AdmitDate_Local as date_acc_admission,
ChiefComplaint as Reason_acc_admission,

date_uACR_index_1 as date_uACR_st4,
uACR_index_1 as uACR_st4,
date_uPCR_index_1 as date_uPCR_st4,
uPCR_index_1 as uPCR_st4 ,


datediff(year,DOB,index_date_2) as age_KFRE,





date_uACR_highest,
uACR_highest,
uPCR_highest,
date_uPCR_highest,

DrugName_1 as SGLT2,
DrugName_1_startdate as SGLT2_start,
DrugName_1_stopdate as SGLT2_stop,

case
when (DrugName_1_startdate is null and DrugName_1_stopdate is null) then null
when (DrugName_1_stopdate is null and IsActive = 0) then round(datediff(day,DrugName_1_startdate,InactiveDate_Local)/30,1)
when (DrugName_1_stopdate is null and IsActive = 1) then round(datediff(day,DrugName_1_startdate, getdate())/30,1)
else round(datediff(day,DrugName_1_startdate,DrugName_1_stopdate)/30,1)
end as SGLT2_duration,

DrugName_2 as ACE_i,
DrugName_2_startdate as ACE_i_start,
DrugName_2_stopdate as ACE_i_stop,

case
when (DrugName_2_startdate is null and DrugName_2_stopdate is null) then null
when (DrugName_2_stopdate is null and IsActive = 0) then round(datediff(day,DrugName_2_startdate,InactiveDate_Local)/30,1)
when (DrugName_2_stopdate is null and IsActive = 1) then round(datediff(day,DrugName_2_startdate, getdate())/30,1)
else round(datediff(day,DrugName_2_startdate,DrugName_2_stopdate)/30,1)
end as ACE_i_duration,

DrugName_3 as ARB,
DrugName_3_startdate as ARB_start,
DrugName_3_stopdate as ARB_stop,

case
when (DrugName_3_startdate is null and DrugName_3_stopdate is null) then null
when (DrugName_3_stopdate is null and IsActive = 0) then round(datediff(day,DrugName_3_startdate,InactiveDate_Local)/30,1)
when (DrugName_3_stopdate is null and IsActive = 1) then round(datediff(day,DrugName_3_startdate, getdate())/30,1)
else round(datediff(day,DrugName_3_startdate,DrugName_3_stopdate)/30,1)
end as ARB_duration,

DrugName_4 as Diltiazem,
DrugName_4_startdate as Diltiazem_start,
DrugName_4_stopdate as Diltiazem_stop,

case
when (DrugName_4_startdate is null and DrugName_4_stopdate is null) then null
when (DrugName_4_stopdate is null and IsActive = 0) then round(datediff(day,DrugName_4_startdate,InactiveDate_Local)/30,1)
when (DrugName_4_stopdate is null and IsActive = 1) then round(datediff(day,DrugName_4_startdate, getdate())/30,1)
else round(datediff(day,DrugName_4_startdate,DrugName_4_stopdate)/30,1)
end as Diltiazem_duration,

MRN,
NHS_no,
[ObjectID] as patient_object_id

from table_9
)

select 
NHS_no,
[FirstName], 
[LastName], 
date_index,
eGFR_index,
DOB,
[Sex],
[Ethnicity],

PrimaryRenalDiagnosis, 
DiabeticStatus, 

DOD,

deceased_or_not,

IsActive, 
InactiveDate_Local,
Latest_Modality,
Date_Latest_Modality,


date_ESRF,
date_ESRF_eGFR,
eGFR_at_ESRF,
first_ESRF_modality,

date_1st_eGFR,
eGFR_1st,
date_1st_prog_note,
first_progress_note_type,
date_1st_prog_note_eGFR,
eGFR_1st_prog_note,

case
when date_ESRF is null or date_1st_prog_note is null then null
else datediff(month,date_1st_prog_note,date_ESRF) 
end as time_1_prog_note_dx,

date_stage_4,
egFR_stage_4,
test_date_1,
test_eGFR_1,

index_date_1_year_prior,
index_eGFR_1_year_prior,
date_index,
eGFR_index,
date_post_index,
eGFR_post_index,

index_2_1_year_date,

index_2_1_year_eGFR,


date_eGFR_latest,
eGFR_latest,

date_lowest_eGFR,
eGFR_lowest,
index_1_to_index_2_time,

fall_1_4_index,
fall_4_index_1_year_prior,
fall_2_index_1yr,
fall_3_index_last,
time_to_ESRF,

delta_1_2,
delta_1_3,
delta_2_3,
delta_2_4,



date_AKC_modality,
date_Low_Clearance_modality,
date_Predialysis_modality,
date_CM_modality,


first_pre_RRT_modality,

first_pre_RRT_modality_date,

modality_egfr.LabTestDate_Local as first_pre_RRT_modality_eGFR_date,
modality_egfr.eGFRNum as first_pre_RRT_modality_eGFR,

date_Dx_Access_Clinic,
date_acc_admission,
Reason_acc_admission,
date_uACR_highest,
uACR_highest,
uPCR_highest,
date_uPCR_highest,
date_uACR_st4,
uACR_st4,
date_uPCR_st4,
uPCR_st4 ,

uACR_conversion_date,
uACR_conversion_val,
gender_KFRE,
age_KFRE,
round(eGFR_index,1) as eGFR_KFRE,


case
when age_KFRE is null or gender_KFRE is null or uACR_conversion_val is null or eGFR_index is null then null
else round(100*(1-power(0.9878,exp(-0.2201*((age_KFRE/10)-7.036) + 0.2467*(gender_KFRE-0.5642) - 0.5567*((eGFR_index/5)-7.222) + 0.451*(log(uACR_conversion_val/0.11312)-5.137)))),1)
end as KFRE_2_year,

case
when age_KFRE is null or gender_KFRE is null or uACR_conversion_val is null or eGFR_index is null then null
else round(100*(1-power(0.9570,exp(-0.2201*((age_KFRE/10)-7.036) + 0.2467*(gender_KFRE-0.5642) - 0.5567*((eGFR_index/5)-7.222) + 0.451*(log(uACR_conversion_val/0.11312)-5.137)))),1)
end as KFRE_5_year,



SGLT2,
SGLT2_start,
SGLT2_stop,

SGLT2_duration,

ACE_i,
ACE_i_start,
ACE_i_stop,

ACE_i_duration,

ARB,
ARB_start,
ARB_stop,

ARB_duration,

Diltiazem,
Diltiazem_start,
Diltiazem_stop,

Diltiazem_duration,

MRN,
NHS_no,
patient_object_id,

[FirstName], 
[LastName]

from table_fin

outer apply (
       select top 1 eGFRNum, PatientObjectId, LabTestDate_Local  from PatientLabPanelRenalAdequacy
       where table_fin.patient_object_id = PatientLabPanelRenalAdequacy.PatientObjectID
	   and LabTestDate_Local <= first_pre_RRT_modality_date
	   and PatientLabPanelRenalAdequacy.eGFRNum is not null
       order by PatientLabPanelRenalAdequacy.LabTestDate_Local desc
) modality_egfr

order by 2,1


;
