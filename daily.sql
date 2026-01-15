# Daily
--Day to day SQL used for patient data collection
USE ProductionOfflineReportingViewsGB;
with DT2 as(
select
FullName,
MedicalRecordNo,
PrimaryRenalModality,
adequacy.LabTestDate_Local ,
adequacy.eGFRNum ,
PrimaryRenalModalityDialysisProvider,
sheet1.LabTestDate_Local as 'year_prior_LabTestDate_Local',
sheet1.eGFRNum as 'year_prior_eGFRNum',
sheet2.LabTestDate_Local as 'not_year_prior_LabTestDate_Local',
sheet2.eGFRNum as 'not_year_prior_eGFRNum'
from RenalPatientActive
cross apply (
  select top 1 PatientObjectId, eGFRNum, LabTestDate_Local  from PatientLabPanelRenalAdequacy
  where RenalPatientActive.ObjectID = PatientLabPanelRenalAdequacy.PatientObjectID
  and RenalPatientActive.PrimaryRenalModality = 'AKC'
  --and PatientLabPanelRenalAdequacy.eGFRNum is not null
  order by PatientLabPanelRenalAdequacy.LabTestDate_Local desc
) adequacy
outer apply (
	select top 1 PatientObjectId, eGFRNum, LabTestDate_Local  from PatientLabPanelRenalAdequacy 
	where RenalPatientActive.ObjectID = PatientLabPanelRenalAdequacy.PatientObjectID
	and PatientLabPanelRenalAdequacy.eGFRNum is not null
	and LabTestDate_Local < dateadd(year,-1,getdate()) 
	order by PatientLabPanelRenalAdequacy.LabTestDate_Local desc
) sheet1
outer apply (
	select top 1 PatientObjectId, eGFRNum, LabTestDate_Local  from PatientLabPanelRenalAdequacy
	where RenalPatientActive.ObjectID = PatientLabPanelRenalAdequacy.PatientObjectID
	and PatientLabPanelRenalAdequacy.eGFRNum is not null
	order by PatientLabPanelRenalAdequacy.LabTestDate_Local asc
) sheet2
)

, DT3 as (
select FullName,MedicalRecordNo as MRN,PrimaryRenalModality, PrimaryRenalModalityDialysisProvider, LabTestDate_Local as Date_creatinine_lf,eGFRNum as eGFR,
case
when year_prior_LabTestDate_Local is null then not_year_prior_LabTestDate_Local
else year_prior_LabTestDate_Local
end as Date_previous_creatinine_lf,
case 
when year_prior_LabTestDate_Local is null then not_year_prior_eGFRNum
else year_prior_eGFRNum
end as previous_eGFR
from DT2
)
 
select 
FullName,
MRN,
PrimaryRenalModality,
PrimaryRenalModalityDialysisProvider as Unit,
CONVERT(VARCHAR, Date_creatinine_lf, 103) as Date_creatinine,
--Date_creatinine_lf,
eGFR, 
CONVERT(VARCHAR, Date_previous_creatinine_lf, 103) as Date_previous_creatinine,
--Date_previous_creatinine_lf,
previous_eGFR,


case
when eGFR is null then null
when Date_creatinine_lf = Date_previous_creatinine_lf then null
else round(365*(eGFR-previous_eGFR)/datediff(day,Date_creatinine_lf,Date_previous_creatinine_lf),1)
end as fall_of_change_eGFR

from DT3
where PrimaryRenalModalityDialysisProvider  like 'Lister%'

order by Unit asc, eGFR asc; 
