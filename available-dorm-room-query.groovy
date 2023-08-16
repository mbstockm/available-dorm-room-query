import com.opencsv.CSVWriter
import com.opencsv.CSVWriterBuilder
import groovy.sql.Sql
import org.apache.commons.mail.Email
import org.apache.commons.mail.MultiPartEmail

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.sql.ResultSet
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

@GrabConfig(systemClassLoader = true)
@Grab(group='com.oracle.ojdbc', module='ojdbc8', version='19.3.0.0')
@Grab(group='com.opencsv', module='opencsv', version='5.8')
@Grab(group='org.apache.commons', module='commons-email', version='1.5')

def rundate = LocalDateTime.now()
def csv = Paths.get(System.properties.'user.dir',"available-dorm-room-query-${rundate.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))}.csv")
// When running script remember to set the following args on the command line or run configuration
def runterm = args[0]
def recipients = args[1]

Sql.withInstance(dbProps()) { sql ->
    availableRoomQuery(sql,runterm) { rs ->
        createCsv(csv,rs)
    }
    sendEmail(recipients,csv,rundate)
    Files.deleteIfExists(csv)
}

def availableRoomQuery(Sql sql, String runterm, Closure c) {
    sql.query("""
                  select term.description term_description
                        ,rd.slbrdef_bldg_code bldg_code
                        ,rd.slbrdef_room_number room_number
                        ,rd.slbrdef_desc room_description
                        ,slbbldg_camp_code campus
                        ,slbbldg_site_code site
                        ,rd.slbrdef_sex room_gender
                        ,slrbcat_code category_code
                        ,slrbcat_desc category_description
                        ,rd.slbrdef_rmst_code room_status
                        ,rd.slbrdef_rrcd_code room_rate
                        ,rd.slbrdef_capacity capacity
                        ,rd.slbrdef_maximum_capacity max_capacity
                        ,count(decode(stvascd_count_in_usage,'Y',ra.slrrasg_pidm,null)) used
                        ,rd.slbrdef_capacity - count(decode(stvascd_count_in_usage,'Y',ra.slrrasg_pidm,null)) remaining 
                        ,listagg(decode(stvascd_count_in_usage,'Y',coalesce(rap.spbpers_pref_first_name,rai.spriden_first_name)
                                                                    ||' '||rai.spriden_last_name
                                                                    ||' ('||rap.spbpers_sex||')'
                                                              ,null)
                           ,',') within group (order by 1) occupants
                    from slbrdef rd, slrbcat, slrrasg ra, spriden rai, spbpers rap, stvascd, slbbldg,
                         (select stvterm_code code, stvterm_desc description 
                            from stvterm) term 
                   where term.code = ${runterm}
                     and rd.slbrdef_term_code_eff = (select max(rd2.slbrdef_term_code_eff) from slbrdef rd2 
                                                      where rd2.slbrdef_bldg_code = rd.slbrdef_bldg_code 
                                                        and rd2.slbrdef_room_number = rd.slbrdef_room_number 
                                                        and rd2.slbrdef_term_code_eff <= term.code) 
                     and rd.slbrdef_rmst_code = 'AC' 
                     and rd.slbrdef_room_type = 'D' 
                     and rd.slbrdef_bldg_code = slrbcat_bldg_code(+) 
                     and rd.slbrdef_bcat_code = slrbcat_code(+) 
                     and term.code = ra.slrrasg_term_code(+) 
                     and rd.slbrdef_bldg_code = ra.slrrasg_bldg_code(+) 
                     and rd.slbrdef_room_number = ra.slrrasg_room_number(+) 
                     and ra.slrrasg_ascd_code = stvascd_code(+) 
                     and slbbldg_bldg_code = rd.slbrdef_bldg_code
                     and slbbldg_camp_code = 'U'         
                     and rai.spriden_change_ind is null
                     and ra.slrrasg_pidm = rai.spriden_pidm(+)
                     and ra.slrrasg_pidm = rap.spbpers_pidm(+)
                  group by term.code 
                          ,term.description 
                          ,rd.slbrdef_bldg_code 
                          ,rd.slbrdef_room_number 
                          ,rd.slbrdef_desc 
                          ,rd.slbrdef_sex 
                          ,rd.slbrdef_capacity
                          ,rd.slbrdef_maximum_capacity
                          ,rd.slbrdef_rmst_code
                          ,rd.slbrdef_rrcd_code
                          ,slrbcat_desc 
                          ,slrbcat_code
                          ,slbbldg_camp_code
                          ,slbbldg_site_code
                  having rd.slbrdef_capacity - count(decode(stvascd_count_in_usage,'Y',ra.slrrasg_pidm,null)) > 0
                  order by bldg_code,room_number
              """,
            c)
}

def createCsv(Path csv, ResultSet rs) {
    csv.withWriter { bw ->
        CSVWriter csvWriter = new CSVWriterBuilder(bw).build()
        csvWriter.writeAll(rs,true)
    }
}

def sendEmail(String recipients, Path csv, LocalDateTime rundate) {
    if (System.properties.'os.name'.toString().toUpperCase().contains('WINDOWS')) {
        println 'Skipped executing email commands while running in windows development environment. localmail host would not be setup.'
        return
    }
    Email email = new MultiPartEmail()
    email.setHostName('localhost')
    email.setSmtpPort(25)
    email.setFrom('noreply@utica.edu')
    email.setSubject("Available Dorm Room Query ${rundate.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))}")
    email.setMsg('Report Attached.')
    email.addTo(recipients.split(','))
    email.attach(csv.toFile())
    email.send()
}

def dbProps() {
    def properties = new Properties();
    Paths.get(System.properties.'user.home','.credentials','bannerProduction.properties').withInputStream {
        properties.load(it)
    }
    return properties
}