using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Data;
using System.IO;
using System.Drawing;
using System.Drawing.Imaging;
using System.Linq;
using Npgsql;
using System.Net.Mail;
using System.Text;

namespace SANBI_SEIS_Image_Job
{
    class Program
    {
        public static ArrayList newProjects = new ArrayList();
        static void Main(string[] args)
        {
            DataTable newImages = NewImages();
            if (newImages != null && newImages.Rows.Count > 0)
            {
                Console.WriteLine(newImages.Rows.Count);
                DataTable unsuccessfulInserts = newImages.Clone();
                DataTable successfulInserts = newImages.Clone();
                ArrayList duplicates = new ArrayList();
                foreach (DataRow row in newImages.Rows)
                {

                    bool success = false;
                    string documentId = row["DocumentId"].ToString();
                    string institutionName = row["Institution"].ToString();
                    string exepeditionName = row["FolderName"].ToString();
                    string imageName = row["DocumentName"].ToString();
                    string imageURL = row["imageURL"].ToString();
                    //Get the SEIS location of the image.
                    //Complete the path of the image by adding the orginal image name. 
                    string path = "";
                    try
                    {
                        var documentDirectory = GetDocumentDirectory(row);
                        path = documentDirectory[0] + @"\orig_" + documentId + ".ims";
                    }
                    catch (Exception e)
                    {
                        WriteErrorToFile(e, path);
                        continue;
                    }

                    //Copy image to the web folder location.
                    if (path != "")
                    {
                        string webLocation = ConfigurationManager.AppSettings["WebLocation"];
                        string toFolder = webLocation + institutionName.Replace(" ", "_") + @"\" + exepeditionName.Replace(" ", "_");
                        string toFolderWithImage = toFolder + @"\" + imageName + ".jpg";
                        if (!duplicates.Contains(toFolderWithImage))
                        {
                            duplicates.Add(toFolderWithImage);
                        }
                        else
                        {
                            Console.WriteLine(toFolderWithImage);
                        }
                        if (!File.Exists(toFolderWithImage))
                        {
                            success = CopyImage(path, imageName, institutionName, exepeditionName);
                        }
                        else
                            success = true;
                    }
                    //If the image copy was successful, insert to PSQL database.
                    if (success)
                    {
                        success = InsertToPsql(institutionName, exepeditionName, imageName, imageURL);
                    }
                    //If copy or insert was not successful, add row to unsucessful DT.
                    if (!success)
                        unsuccessfulInserts.ImportRow(row);
                    else
                        successfulInserts.ImportRow(row);
                }
                SendEmails(successfulInserts, null);
                SendEmails(null, unsuccessfulInserts);
                Console.ReadLine();
            }
        }

        protected static void SendEmails(DataTable successfulInserts, DataTable unsuccessfulInserts)
        {
            string smtpClient = ConfigurationManager.AppSettings["SmtpClient"];
            string smtpUserName = ConfigurationManager.AppSettings["SmtpUserName"];
            string smtpPassword = ConfigurationManager.AppSettings["SmtpPassword"];
            string emailFrom = ConfigurationManager.AppSettings["EmailFromAddress"];

            System.Net.Mail.MailMessage Msg = new MailMessage();
            SmtpClient smtp = new SmtpClient(smtpClient);
            Msg.From = new MailAddress(emailFrom);

            //If there are successful inserts
            if (successfulInserts != null && successfulInserts.Rows.Count > 0)
            {
                string[] emails = EmailAddresses("Email");
                foreach (string email in emails)
                {
                    Msg.To.Add(email);
                    Msg.Subject = "Successfully inserted images.";
                    Msg.Body = BuildEmailBody(successfulInserts, null);
                }
            }
            //If there are unsuccessful inserts.
            if (unsuccessfulInserts != null && unsuccessfulInserts.Rows.Count > 0)
            {
                string[] emails = EmailAddresses("SysAdminEmail");
                foreach (string email in emails)
                {
                    Msg.To.Add(email);
                    Msg.Subject = "Unsuccessful Inserts.";
                    Msg.Body = BuildEmailBody(null, unsuccessfulInserts);
                }
            }
            //If there are no unsuccessful inserts, write date to a file.
            else if (unsuccessfulInserts != null && unsuccessfulInserts.Rows.Count == 0)
            {
                WriteSuccessfulDateToFile();
            }

            Msg.IsBodyHtml = true;
            smtp.Port = 587;
            smtp.Credentials = new System.Net.NetworkCredential(smtpUserName, smtpPassword);

            try
            {
                smtp.Send(Msg);
            }
            catch (Exception e)
            {
                WriteErrorToFile(e, "Send Emails");
            }
        }

        protected static string[] EmailAddresses(string webConfigSetting)
        {
            string[] emails = ConfigurationManager.AppSettings[webConfigSetting].Split(';');
            return emails;
        }

        protected static string BuildEmailBody(DataTable successfulInserts, DataTable unsuccessfulInserts)
        {
            string body = string.Empty;

            if (successfulInserts != null && successfulInserts.Rows.Count > 0)
            {
                body = "Hi, <br/><br/>" + successfulInserts.Rows.Count +
                       " new tasks have been inserted into the database. <br/><br/>";

                if (newProjects.Count > 0)
                {
                    body += newProjects.Count + " new projects have been created: ";
                    body = newProjects.Cast<string>().Aggregate(body, (current, project) => current + ("<br/>" + project));
                    body += "<br/><br/>Projects need to be activated before used. Please make sure to link the project to an institution.";
                }

                body += "<br/><br/> Regards.";
            }
            else if (unsuccessfulInserts != null && unsuccessfulInserts.Rows.Count > 0)
            {
                body = "Hi, <br/><br/>" + unsuccessfulInserts.Rows.Count +
                       " tasks were unsuccessfully inserted into the database. Please see error log for a detailed report.";

                body += "<br/><br/> Regards.";
            }

            return body;
        }


        protected static bool InsertToPsql(string institutionName, string exepeditionName, string imageName, string imageURL)
        {
            imageURL += ".jpg";
            int? institutionId = null;
            int projectId = 0;
            //when project find = true then the projectId that will be used to add the task to has been found. 
            bool foundProjectId = false;
            var maxTaskCount = ConfigurationManager.AppSettings["ExpeditionSize"];
            //Keep track of the area in the method for the error log incase of error.
            string errorAreaForLog = "Connection";
            try
            {
                string connectionString = ConfigurationManager.AppSettings["PGSQLConnection"];
                using (
                    NpgsqlConnection conn =
                        new NpgsqlConnection(connectionString))
                {
                    conn.Open();

                    //Check to see if task has already been inserted into the database:
                    NpgsqlCommand cmd = new NpgsqlCommand("SELECT Id FROM multimedia WHERE file_path = " + "'" + imageURL + "'", conn);
                    NpgsqlDataReader rd = cmd.ExecuteReader();
                    bool hasRows = rd.HasRows;
                    rd.Close();
                    //If there is no row return it means the task has not been inserted into the database so we can continue.
                    if (!hasRows)
                    {
                        //Get the Institution Id
                        errorAreaForLog = "Select Institution Id";
                        cmd = new NpgsqlCommand("SELECT Id FROM Institution WHERE name = " + "'" + institutionName.Trim() + "'", conn);
                        rd = cmd.ExecuteReader();
                        if (rd.HasRows)
                        {
                            rd.Read();
                            institutionId = rd.GetInt32(0);
                        }
                        rd.Close();

                        //Project count is set default to 1. If an expedition is not found it will +1 and look for expedition 2, then expedition 3 etc.
                        int projectCount = 1;
                        string newExpeditionName = exepeditionName;
                        while (!foundProjectId)
                        {
                            //modify the name to look for the next expedition as expedition x.
                            if (projectCount != 1)
                            {
                                newExpeditionName = exepeditionName + " " + projectCount;
                            }

                            errorAreaForLog = "Select from Project";

                            cmd = new NpgsqlCommand("SELECT * FROM project WHERE name = @ExpeditionName AND institution_id = @InstitutionId", conn);
                            cmd.Parameters.AddWithValue("@ExpeditionName", newExpeditionName);
                            cmd.Parameters.AddWithValue("@InstitutionId", institutionId ?? Convert.DBNull);
                            rd = cmd.ExecuteReader();
                            projectId = DataReaderResultReturnId(rd);


                            //Get the number of tasks there are in the project and compare it to the config allowed amount. 
                            if (projectId != 0)
                            {
                                errorAreaForLog = "Select No. Of tasks in project";
                                cmd = new NpgsqlCommand("SELECT COUNT(*) FROM task WHERE project_id = " + projectId, conn);
                                rd = cmd.ExecuteReader();
                                int taskCount = DataReaderResultReturnId(rd);
                                if (taskCount < int.Parse(maxTaskCount))
                                {
                                    foundProjectId = true;
                                }
                                else
                                {
                                    projectCount++;
                                }
                            }
                            else
                            {
                                foundProjectId = true;
                                projectId = CreateProject(conn, newExpeditionName, institutionId);
                            }

                        }

                        //Project has been found/Create. Task can be inserted into database.
                        return InsertTask(conn, imageName, projectId, imageURL);
                    }
                    return true;
                }
            }
            catch (Exception e)
            {
                WriteErrorToFile(e, errorAreaForLog);
                return false;
            }
        }

        protected static bool InsertTask(NpgsqlConnection conn, string imageName, int projectId, string imageURL)
        {
            string errorArea = "maxTaskId";
            try
            {
                //Get lastest task Id and +1
                NpgsqlCommand cmd = new NpgsqlCommand("SELECT MAX(id) FROM task", conn);
                NpgsqlDataReader rd = cmd.ExecuteReader();
                int newTaskId = 0;
                try
                {
                    newTaskId = DataReaderResultReturnId(rd) + 1;
                }
                catch (Exception)
                {
                    newTaskId = 80000;
                    rd.Close();
                }

                errorArea = "TaskInsert";
                //Insert task to database.
                cmd = new NpgsqlCommand(@"INSERT INTO task(
                                                    id, created, date_fully_transcribed, date_fully_validated, date_last_updated, 
                                                    external_identifier, external_url, fully_transcribed_by, fully_validated_by, 
                                                    is_valid, last_viewed, last_viewed_by, project_id, viewed) 
                                                    VALUES (@TaskId, null, null, null, null, @ImageName, null, null, null, null, 0, 0, @ProjectId, 0);", conn);
                cmd.Parameters.AddWithValue("@TaskId", newTaskId);
                cmd.Parameters.AddWithValue("@ImageName", imageName);
                cmd.Parameters.AddWithValue("@ProjectId", projectId);
                cmd.ExecuteNonQuery();

                errorArea = "MaxMultimediaId";
                //Get laest multimedia Id
                cmd = new NpgsqlCommand("SELECT MAX(id) FROM multimedia", conn);
                rd = cmd.ExecuteReader();
                int newMultimediaId = 0;
                try
                {
                    newMultimediaId = DataReaderResultReturnId(rd) + 1;

                }
                catch (Exception)
                {
                    newMultimediaId = 80000;
                    rd.Close();
                }

                errorArea = "multimediaInsert";
                //Insert multimedia to database.
                cmd = new NpgsqlCommand("INSERT INTO multimedia(id, created, creator, file_path, file_path_to_thumbnail, licence, mime_type, task_id)" +
                                        "VALUES (@MultimediaId, current_date, null, @ImageURL, @ImageURL, null, 'image/jpeg', @TaskId)", conn);
                cmd.Parameters.AddWithValue("@TaskId", newTaskId);
                cmd.Parameters.AddWithValue("@MultimediaId", newMultimediaId);
                cmd.Parameters.AddWithValue("@ImageURL", imageURL);
                cmd.ExecuteNonQuery();
                Console.WriteLine("Inserted image " + imageURL + " to PSQL");
                Exception er = new Exception();
                WriteErrorToFile(er, "New task inserted " + imageURL);
                return true;
            }
            catch (Exception e)
            {
                WriteErrorToFile(e, errorArea);
                return false;
            }
        }


        protected static int CreateProject(NpgsqlConnection conn, string expeditionName, int? institutionId)
        {
            int projectId = 0;
            try
            {
                //Get latest project Id and + 1
                NpgsqlCommand cmd = new NpgsqlCommand("SELECT MAX(id) FROM project", conn);
                NpgsqlDataReader rd = cmd.ExecuteReader();
                projectId = DataReaderResultReturnId(rd) + 1;
                //Insert the project to PSQL database.
                cmd = new NpgsqlCommand(@"INSERT INTO project(
                                                    id, version, background_image_attribution, background_image_overlay_colour, 
                                                    collection_event_lookup_collection_code, created, description, 
                                                    disable_news_items, featured_image_copyright, featured_label, 
                                                    featured_owner, harvestable_by_ala, inactive, institution_id, 
                                                    leader_icon_index, locality_lookup_collection_code, map_init_latitude, 
                                                    map_init_longitude, map_init_zoom_level, name, picklist_institution_code, 
                                                    project_type_id, short_description, show_map, template_id, tutorial_links)
                                                    VALUES (@ProjectId , 0, '', '', '', current_date, 'Expedition Description', FALSE, '', @ExpeditionName, '', TRUE, TRUE, @InstitutionId, 0, '', '-57.4023613632533', '176.396484625', 1, @ExpeditionName ,null, 7134, 'Short Description', FALSE, NULL, '')", conn);
                cmd.Parameters.AddWithValue("@ProjectId", projectId);
                cmd.Parameters.AddWithValue("@ExpeditionName", expeditionName);
                cmd.Parameters.AddWithValue("@InstitutionId", institutionId ?? Convert.DBNull);
                cmd.ExecuteNonQuery();
                newProjects.Add(expeditionName);
            }
            catch (Exception e)
            {
                WriteErrorToFile(e, "CreateExpedition");
            }
            return projectId;
        }

        /// <summary>
        /// Pass through a SQL Data Reader and return the first column of the first row if it exists. 
        /// </summary>
        /// <param name="rd">SQL Data Reader containting the information.</param>
        /// <returns></returns>
        protected static int DataReaderResultReturnId(NpgsqlDataReader rd)
        {
            int id = 0;
            if (rd != null && rd.HasRows)
            {
                rd.Read();
                id = rd.GetInt32(0);
            }
            rd.Close();
            return id;

        }


        /// <summary>
        /// Copies the image from the SEIS location to the ROOT folder of the application. 
        /// </summary>
        /// <param name="fromPath">SEIS image location</param>
        /// <param name="imageName">Name of the image</param>
        /// <param name="institutionName">Name of the institution</param>
        /// <param name="expeditionName">Name of the expedition</param>
        /// <returns>Is successful copy.</returns>
        protected static bool CopyImage(string fromPath, string imageName, string institutionName, string expeditionName)
        {
            try
            {
                //Get maximum image size from config and multiply by 1000000 for conversion to bytes
                int maxImageSize = 2000000; //Default 2mb
                if (ConfigurationManager.AppSettings["maxImageSize"] != null)
                {
                    try
                    {
                        maxImageSize = int.Parse(ConfigurationManager.AppSettings["maxImageSize"]) * 1000000;
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Incorrect setting used. Default is being used intstead.");
                    }
                }

                string webLocation = ConfigurationManager.AppSettings["WebLocation"];
                string toFolder = webLocation + institutionName.Replace(" ", "_") + @"\" + expeditionName.Replace(" ", "_");
                System.IO.Directory.CreateDirectory(toFolder);
                string toFolderWithImage = toFolder + @"\" + imageName + ".jpg";
                //If file size is greater than max file size. 
                if (!File.Exists(toFolderWithImage))
                {
                    Image image = Image.FromFile(fromPath);
                    Console.WriteLine("Opened file " + fromPath);
                    var fileLength = new FileInfo(fromPath).Length;
                    if (fileLength > maxImageSize)
                    {
                        //Resize to make it smaller.
                        using (var newImage = ScaleImage(image))
                        {
                            newImage.Save(toFolderWithImage, ImageFormat.Jpeg);
                        }
                    }
                    else
                    {
                        image.Save(toFolderWithImage, ImageFormat.Jpeg);
                    }
                    image.Dispose();
                    return true;
                }
                else
                {
                    return false;
                }

            }
            catch (Exception e)
            {
                Console.WriteLine("Did not open " + fromPath);
                WriteErrorToFile(e, "CopyImage, image: " + fromPath);
            }

            return false;
        }

        public static Image ScaleImage(Image image)
        {
            int maxWidth = 2500; //Default
            if (ConfigurationManager.AppSettings["maxImageWidthResolution"] != null)
            {
                try
                {
                    maxWidth = int.Parse(ConfigurationManager.AppSettings["maxImageWidthResolution"]);
                }
                catch (Exception e)
                {
                    Console.WriteLine("Incorrect setting used. Default is being used intstead.");
                }
            }

            int maxHeight = 1600; //Default
            if (ConfigurationManager.AppSettings["maxImageHeightResolution"] != null)
            {
                try
                {
                    maxHeight = int.Parse(ConfigurationManager.AppSettings["maxImageHeightResolution"]);
                }
                catch (Exception e)
                {
                    Console.WriteLine("Incorrect setting used. Default is being used intstead.");
                }
            }
            var ratioX = (double)maxWidth / image.Width;
            var ratioY = (double)maxHeight / image.Height;
            var ratio = Math.Min(ratioX, ratioY);

            var newWidth = (int)(image.Width * ratio);
            var newHeight = (int)(image.Height * ratio);

            var newImage = new Bitmap(newWidth, newHeight);

            using (var graphics = Graphics.FromImage(newImage))
                graphics.DrawImage(image, 0, 0, newWidth, newHeight);

            return newImage;
        }

        /// <summary>
        /// Returns the full path of the image location. 
        /// </summary>
        /// <param name="document">document DataRow.</param>
        /// <returns></returns>
        protected static string[] GetDocumentDirectory(DataRow document)
        {
            try
            {
                string documentId = document["DocumentId"].ToString();
                string imageLocation = ConfigurationManager.AppSettings["ImageLocation"];
                var foldersFound = Directory.GetDirectories(imageLocation, documentId, SearchOption.AllDirectories);
                return foldersFound;
            }
            catch (Exception e)
            {
                WriteErrorToFile(e, "GetDocumentDirectory");
            }
            return null;
        }

        /// <summary>
        /// Gets all new images from the database. 
        /// </summary>
        /// <returns></returns>
        protected static DataTable NewImages()
        {
            var dt = new DataTable();
            using (var conn = new SqlConnection())
            {
                try
                {
                    string lastRunDate = getLastRunDate();
                    conn.ConnectionString = ConfigurationManager.AppSettings["MSSQLConnection"];
                    conn.Open();
                    string selectScript = @"select 
                                         ims_document.ims_id as 'DocumentId',
                                         ims_document.ims_name as 'DocumentName',
                                         folder.ims_id as 'FolderId',
                                         childFolder.ims_name as 'FolderName',
                                         parentFolder.ims_name as Institution,
                                         folder.ims_parent_folder as 'ParentFolderId',
                                         ('http://196.21.170.68/webgate/resources/Digivol/' + replace(parentFolder.ims_name, ' ', '_') + '/' + replace(childFolder.ims_name, ' ', '_') + '/'+ ims_document.ims_name) as 'ImageURL'
                                         from ims_document
                                         inner join ims_folder folder on ims_document.ims_folder = folder.ims_id
                                         inner join ims_folder_add_lang childFolder on folder.ims_id = childFolder.ims_folder
                                         inner join ims_folder_add_lang parentFolder on folder.ims_parent_folder = parentFolder.ims_folder
                                         where ims_document.ims_upload_date > '" + lastRunDate +
                                         "' and (select ims_folder.ims_parent_folder from ims_folder where ims_folder.ims_id = parentFolder.ims_folder) = 951";

                    var command = new SqlCommand(selectScript, conn);
                    dt.Load(command.ExecuteReader());
                }
                catch (Exception e)
                {
                    WriteErrorToFile(e, "NewImages");
                }
            }
            return dt;
        }

        protected static string getLastRunDate()
        {
            string filePath = AppDomain.CurrentDomain.BaseDirectory + "LastSuccessfulRun.txt";
            string date = string.Empty;
            using (StreamReader sr = new StreamReader(filePath))
            {
                date = sr.ReadLine();
            }
            //Return the date or DateTime.Now if date in file is null. 
            return date ?? (date = DateTime.Now.ToString());
        }

        /// <summary>
        /// Writes error to file when an exception is caught. 
        /// </summary>
        /// <param name="e">The caught exception.</param>
        /// <param name="method">What method the error occured in.</param>
        protected static void WriteErrorToFile(Exception e, string method)
        {
            string filePath = AppDomain.CurrentDomain.BaseDirectory + "Error.txt";
            string error = "Date: " + DateTime.Now + " Method: " + method + " Error: " + e.ToString();

            using (StreamWriter sw = new StreamWriter(filePath, true))
            {
                sw.WriteLine(error);
                sw.Close();
            }
        }

        /// <summary>
        /// Writes the latest date where all task creations were successful.
        /// </summary>
        protected static void WriteSuccessfulDateToFile()
        {
            string filePath = AppDomain.CurrentDomain.BaseDirectory + "LastSuccessfulRun.txt";
            using (StreamWriter sw = new StreamWriter(filePath, false))
            {
                sw.Write(DateTime.Now);
                sw.Close();
            }

        }

    }

}
