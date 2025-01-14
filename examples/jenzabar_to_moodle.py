# file: examples/jenzabar_to_moodle.py

import keyring

from moodle_sync.course import CourseSync
from moodle_sync.enrolment import EnrolmentSync
from moodle_sync.user import UserSync
from moodle_sync.provider_moodleapi import MoodleAPICourseProvider, MoodleAPIEnrolmentProvider, MoodleAPIUserProvider
from moodle_sync.provider_mssql import MoodleMSSQLCourseProvider, MoodleMSSQLEnrolmentProvider
from moodle_sync.provider_mysql import MoodleMySQLCourseProvider, MoodleMySQLEnrolmentProvider
from moodle_sync.provider_mssql import MoodleMSSQLUserProvider

from moodle_sync.config import config

jenzabar_j1_enrollment_view_example = r"""
CREATE VIEW [dbo].[_moodle_enrollments] AS
/*
This view pulls a list of course enrollments with columns compatible with moodle data from Jenzabar J1 ERP.

It included cancelled classes that have an instructor assigned, so that Moodle can unenrol all the studetns from those classes.

Note that the UK spelling of enrolment. That's the way it's spelled in Moodle.
select * from _wwn_moodle_enrollments  
SELECT shortname, role, username FROM _moodle_enrollments
*/
	
  with 
  rawcoursedata as (
	SELECT 
	parent_formatted_crs_cde
	,PARSENAME(REPLACE(parent_formatted_crs_cde, '-', '.'), 3) as crs_comp1
	,LEFT((PARSENAME(REPLACE(parent_formatted_crs_cde, '-', '.'), 3)  + REPLICATE('-', 5)), 5) as crs_comp1_padded
    ,LEFT((PARSENAME(REPLACE(parent_formatted_crs_cde, '-', '.'), 3)  + REPLICATE(' ', 5)), 5) as crs_comp1_spaced
	,PARSENAME(REPLACE(parent_formatted_crs_cde, '-', '.'), 2) as crs_comp2
    ,PARSENAME(REPLACE(parent_formatted_crs_cde, '-', '.'), 1) as crs_comp3
	 -- REPLACE(CRS_COMP1, ' ', '') as crs_comp1
	 --,REPLACE(CRS_COMP1, ' ', '-') as crs_comp1_padded
	 --,REPLACE(CRS_COMP2, ' ', '') as crs_comp2
	 --,REPLACE(CRS_COMP3, ' ', '') as crs_comp3
	 --,REPLACE(CRS_COMP4, ' ', '') as crs_comp4
	 --,REPLACE(CRS_COMP5, ' ', '') as crs_comp5
	 ,crs_cde as child_crs_cde
	 ,YR_CDE
	 ,TRM_CDE
	 ,course_text
	 ,FIRST_BEGIN_DTE  
	 ,LAST_END_DTE 
	 ,crs_title
	 ,INSTITUT_DIV_CDE
	 ,CAST(yr_cde AS VARCHAR) + '-' + FORMAT((yr_cde % 1000 + 1), '00') + ' ' + TRM_CDE  AS categoryname
	 ,trm_cde + ' ' + format(yr_cde%1000, '00') + '-' + format((yr_cde % 1000 + 1), '00') as nice_yr_trm
	 ,LEAD_INSTRUCTR_ID
	 ,course_state
	FROM section_master_v
	where 
		-- General Jenzabar conditions:
		course_state != 'Completed' --and course_state != 'Canceled'  we need to  unenroll from cancelled classes
		-- select * from section_master_v where course_state like '%Ca%' and lead_instructr_id is not null and lead_instructr_id != 304340 and yr_cde = 2024
		-- and CRS_ENROLLMENT > 0      -- include courses that have no students, but we would want to avoid creating those courses.
		and first_begin_dte > DATEADD(YEAR, -1, GETDATE()) -- only return courses within the past year.   Moodle sync will do further selecting.
		and last_end_dte >= GETDATE()    -- stop syncing courses after they have ended.
		and (LEAD_INSTRUCTR_ID is not NULL or course_state = 'Canceled')
	)
	
	, coursedata as (
	select -- compute the moodle fields and add also child_crs_cde
	    -- when migrating to the new sync, Warren wilson is redoing shortnames for courses after December 2024.
		-- Adjust this to calculate your course shortname.
		crs_comp1 + '-' + crs_comp2 + '-' + crs_comp3 + ' ' + yr_cde + trm_cde as shortname
		,crs_comp1 + '-' + crs_comp2 + '-' + crs_comp3 +'-' + yr_cde + trm_cde as idnumber
		,categoryname
		,fullname = crs_title + ' (' + crs_comp1 + ' ' + crs_comp2 + ' ' + crs_comp3 + ', ' + nice_yr_trm + ')'
		,summary = course_text
		,[format] = 'weeks'
		,numsections = DATEDIFF(WEEK, FIRST_BEGIN_DTE, LAST_END_DTE) + 1
		,showgrades = 1
		,newsitems = 5
		,startdate = FIRST_BEGIN_DTE   -- keep dates in mssql format, convert in python.
		,enddate   = LAST_END_DTE   
		,automaticenddate = 0  -- a courseformatoption
		,visible=0
		,child_crs_cde   -- these are necessary in the view so that we can sync enrollment based on this particular course.
		,yr_cde, trm_cde
		,lead_instructor_idnum = LEAD_INSTRUCTR_ID -- this pulls the lead instructor from theclass
		,course_state
	from rawcoursedata
		-- Institution Specific conditions:
		where  1=1
		and  LEAD_INSTRUCTR_ID != NNNNNN  -- TBD instructor
		-- these parts of the view are specific to Warren Wilson college to keep certain courses out of the sync.
		and  crs_comp1 not in ('EXCLUDE1', 'EXCLUDE2')  -- exclude service learning and billing enrollment course
		and trm_cde not in ('EXCLUDE_TRM1','TM')  -- exclude particularl trm codes
		--and crs_comp2 != '0000'  and CRS_TITLE != 'CANCELLED' -- any other exclude conditions you want

	)
	/* -- CTE testing block
	select  
	-- for testing, limit number of courses:
	--top 10
	 *
	from coursedata
	where 1=1 
	-- temporary - select courses for a parent/child course:
	-- and (shortname like '%PARENT_CODE%' or child_crs_cde like '%CHILD_CODE%' or child_crs_cde like '%OTHER_CHILD_CODE%')
	*/
, result as (
	select 
		shortname
		,rolename = 'student' 
		,username = tws.web_login
		,child_crs_cde = coursedata.child_crs_cde
		,course_state
		,sch.id_num
		,lead_instructor_idnum
		,instructor_username = tws_instr.web_login
		,started = CASE WHEN DATEADD(day, 5,startdate) <= GETDATE() THEN 1 ELSE 0 END
	from coursedata
	-- cancelled classes will have no student enrollment. So left join.
	left join student_crs_hist sch on sch.crs_cde = coursedata.child_crs_cde and sch.yr_cde = coursedata.yr_cde and sch.trm_cde = coursedata.trm_cde
		and TRANSACTION_STS = 'C'  -- only list current enrollments. Not historical, or dropped or withdrawn.
	left join tw_web_security tws on tws.id_num = sch.id_num
	join tw_web_security tws_instr on tws_instr.id_num = lead_instructor_idnum  
	-- only select people with that have institution email
	-- join address_master am on am.id_num = sch.id_num and am.addr_cde = '*EML' and am.addr_line_1 like '%your-institution.edu%'
	-- cancelled courses that had moodle courses created will still have a lead_instructr_id
)
,result_with_rows as ( 
    -- this is useful for enumerating duplicate rows.
	select 
	    r = ROW_NUMBER() over (partition by shortname, rolename, username order by (select NULL)),
		shortname
		,rolename
		,username
		,child_crs_cde
		,course_state
		,started
		,id_num
		,lead_instructor_idnum
		,instructor_username
	from result

)
, students_and_instructors as (
select 
     shortname, rolename, username,  course_state , started, 
	 crs_cde = child_crs_cde,id_num 
	from result_with_rows where username is not NULL -- classes that have no enrollment of students
union
select distinct 
    shortname,  'editingteacher' as rolename, username=instructor_username,  course_state, started,
	crs_cde = child_crs_cde, id_num = lead_instructor_idnum 
	from result_with_rows
)

select
   shortname, rolename as [role], username, course_state as course_status, started, crs_cde, id_num
   from students_and_instructors 

"""

jenzabar_j1_course_view_example = r"""
    
CREATE VIEW [dbo].[_moodle_courses]  AS
    
/*
This view pulls a list of courses that need to be created in and synced to Moodle with the Moodle Course API sync.
The view must provide column names that map to Moodle Data.  Other columns will be ignored.
In fact, one of the columns, identifying the child course ID, is used for enrollment sync 
so that Moodle can create one parent course for all the child courses
and sync enrollments together.

*/
	
  with rawcoursedata as (
	SELECT 
	  REPLACE(CRS_COMP1, ' ', '') as crs_comp1
	 ,REPLACE(CRS_COMP1, ' ', '-') as crs_comp1_padded
	 ,REPLACE(CRS_COMP2, ' ', '') as crs_comp2
	 ,REPLACE(CRS_COMP3, ' ', '') as crs_comp3
	 ,REPLACE(CRS_COMP4, ' ', '') as crs_comp4
	 ,REPLACE(CRS_COMP5, ' ', '') as crs_comp5
	 ,crs_cde as child_crs_cde
	 ,YR_CDE
	 ,TRM_CDE
	 ,course_text
	 ,FIRST_BEGIN_DTE  
	 ,LAST_END_DTE 
	 ,crs_title
	 ,INSTITUT_DIV_CDE
	 ,CAST(yr_cde AS VARCHAR) + '-' + FORMAT((yr_cde % 1000 + 1), '00') + ' ' + TRM_CDE  AS categoryname
	 ,trm_cde + ' ' + format(yr_cde%1000, '00') + '-' + format((yr_cde % 1000 + 1), '00') as nice_yr_trm
	 ,LEAD_INSTRUCTR_ID
	 -- select top 1 *
	FROM section_master_v
	where 
		-- General Jenzabar conditions:
		course_state != 'Completed' and course_state != 'Canceled'
		-- and CRS_ENROLLMENT > 0      -- include courses that have no students, but we would want to avoid creating those courses.
		and first_begin_dte > DATEADD(YEAR, -1, GETDATE()) -- only return courses within the past year.   Moodle sync will do further selecting.
		and last_end_dte >= GETDATE() -- stop syncing courses after they have ended.
		and LEAD_INSTRUCTR_ID is not NULL
		-- Institution Specific conditions:
		and LEAD_INSTRUCTR_ID != 304340  -- TBD instructor
		and  crs_comp1 not in ('EXCL1', 'EXCL2')  -- exclude service learning and billing enrollment course
		and trm_cde not in ('T1','T2')  -- exclude MFA courses
		and crs_comp2 != '0000'  and CRS_TITLE != 'CANCELLED'
	)
	
	, coursedata as (
	select -- compute the moodle fields and add also child_crs_cde
		-- Here you must compute components for determining the course shortname.
		-- In the case of Warren Wilson, migrating to this sync after Dec 2024, we are changing the format for shortnames a bit.
		-- You can adjust other course settings as desired.
		crs_comp1 + '-' + crs_comp2 + '-' + crs_comp3 + ' ' + yr_cde + trm_cdeas as shortname
		,crs_comp1 + '-' + crs_comp2 + '-' + crs_comp3 +'-' + yr_cde + trm_cde as idnumber
		,categoryname
		,fullname  = crs_title + ' (' + crs_comp1 + ' ' + crs_comp2 + ' ' + crs_comp3 + ', ' + nice_yr_trm + ')'
		,summary = course_text
		,[format] = 'weeks'
		,numsections = DATEDIFF(WEEK, FIRST_BEGIN_DTE, LAST_END_DTE) + 1
		,showgrades = 1
		,newsitems = 5
		,startdate = FIRST_BEGIN_DTE   -- keep dates in mssql format, convert in python.
		,enddate   = LAST_END_DTE   
		,automaticenddate = 0  -- a courseformatoption
		,visible=0
		,child_crs_cde   -- these are necessary in the view so that we can sync enrollment based on this particular course.
		,yr_cde, trm_cde
		,lead_instructor_idnum = LEAD_INSTRUCTR_ID -- this pulls the lead instructor from theclass
	from rawcoursedata


	)
	select  
	-- for testing, limit number of courses:
	-- top 100
	*
	from coursedata



/*
					'categoryid': 13, 
                    'fullname': 'Senior Seminar:Hist/Pol.Sci (HIS--NNNN-F00, 2019-FA) ', 
                    'displayname': 'Senior Seminar:Hist/Pol.Sci (HIS--NNNN-F00, 2019-FA) ', 
                    'idnumber': 'HIS--NNNN_2019-FA_F00', 
                    'summary': 'This course is a senior seminar', 
                    'summaryformat': 1, 
                    'format': 'weeks', 
                    'showgrades': 1, 
                    'newsitems': 5, 
                    'startdate': 1566792000, 
                    'enddate': 1576476000, 
                    'numsections': 16, 
                    'maxbytes': 20971520, 
                    'showreports': 0, 
                    'visible': 0, 
                    'hiddensections': 0, 
                    'groupmode': 0, 
                    'groupmodeforce': 0, 
                    'defaultgroupingid': 0, 
                    'timecreated': 1501277513, 
                    'timemodified': 1566920251, 
                    'enablecompletion': 0, 
                    'completionnotify': 0, 
                    'lang': '', 
                    'forcetheme': '', 
                    'courseformatoptions': [{'name': 'hiddensections', 'value': 0}, {'name': 'coursedisplay', 'value': 0}, {'name': 'automaticenddate', 'value': 0}], 
                    'showactivitydates': False, 
                    'showcompletionconditions': None

	*/


"""

_jenzabar_users_view_example = r"""
    CREATE VIEW _moodle_users AS
    /*
    2024-08	mdl	Create view for manual user sync to Moodle before course enrollment
      Will not create a Moodle user till their *eml is updated with institution email
    */
    with users as (
        select
           nm.id_num as id,
           coalesce(nm.PREFERRED_NAME, nm.first_name) as firstname,
           nm.LAST_NAME as lastname,
           'ldap' as auth,
           am.addr_line_1 as email,
           SUBSTRING(am.addr_line_1, 1, CHARINDEX('@', am.addr_line_1) - 1) as username
           from name_master nm
           join address_master am on nm.id_num = am.id_num and am.addr_cde = '*eml' and am.addr_line_1 like '%example.edu%'
    )
    , moodle_users as (
        select distinct username from _moodle_enrollments
    )
    
    select id, firstname, lastname, auth, email, users.username, 'notused' as password
       from users
       join moodle_users ms on ms.username = users.username

"""

def get_mysql_connection_parameters(db:str, site:str):
    import socket
    mysqlhost, mysqlpassword = 'yoursite.edu', None
    hostname = socket.gethostname()
    mysqluser = 'mydevmachine' if hostname == 'mymachinename' else 'servicemachine'
    mysqlhost = site
    mysqlpassword = keyring.get_password(mysqlhost, mysqluser)
    if not mysqlpassword:
        raise ValueError(f"Password not found for {mysqluser}@{mysqlhost}")
    return dict(host=mysqlhost, user=mysqluser, password=mysqlpassword, database=db)



if __name__ == '__main__':  # Are you running this file directly?  (VS importing it)
    site = 'example.edu'
    moodle_mysql_db = 'moodle_test' if 'test' in site else 'moodle_main'
    api_key = keyring.get_password(site, 'moodle_api')
    # to set api key:   keyring.set_password(site, 'moodle_api', 'your_api_key_here')
    moodle_provider = MoodleAPICourseProvider(site, api_key)
    # test the API connection
    # test_category = 'Misc'
    # moodle_provider.get_category(test_category)


    (j1_server, j1_database) = "jz_database", 'tmseprd',
    (j1_course_view, j1_enroll_view, j1_user_view) = '_moodle_courses', '_moodle_enrollments', '_moodle_users'
    # connection string for pyodbc:
    mssql_connection_string = f"DRIVER=SQL Server;SERVER={j1_server};DATABASE={j1_database};Trusted_Connection=yes;"

    config.debug = True
    config.dryrun = False


    print(f"Beginning Sync for {site}")
    do_users = True
    do_courses = True
    do_enrollments = True

    if do_users:
        jenzabar_user_provider = MoodleMSSQLUserProvider(mssql_connection_string, j1_user_view)
        moodle_user_provider = MoodleAPIUserProvider(site, api_key)
        user_syncer = UserSync(target=moodle_user_provider, source=jenzabar_user_provider)
        user_syncer.sync()

    if do_courses:
        moodle_provider.get_all_courses = False
        jenzabar_provider = MoodleMSSQLCourseProvider(mssql_connection_string, j1_course_view)

        course_syncer = CourseSync(target=moodle_provider, source=jenzabar_provider)
        #moodle_provider.fields_to_update.append('automaticenddate')
        #moodle_provider.fields.append('automaticenddate')

        # You can of course subclass and override the behavior for any of the classes.
        course_syncer.sync_to_moodle()


    if do_enrollments:
        # Now do the enrollments.  the API has problems deleting / unenrolling users.
        # So we are using a mysql database provider directly with Moodle.
        mysql_connection_params = get_mysql_connection_parameters(moodle_mysql_db, site=site)

        moodle_enr_provider = MoodleMySQLEnrolmentProvider(**mysql_connection_params)
        jbar_enr_provider = MoodleMSSQLEnrolmentProvider(mssql_connection_string, j1_enroll_view)

        enrol_syncer = EnrolmentSync(target=moodle_enr_provider, source=jbar_enr_provider)

        enrol_syncer.sync_to_moodle()

