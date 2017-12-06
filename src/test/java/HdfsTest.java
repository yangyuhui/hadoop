import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.InputStream;
import java.net.URI;

/**
 * Created by yyh on 2017/11/1.
 */
public class HdfsTest {
	FileSystem fs;
	@Before
	public void init() throws Exception{
		System.setProperty("HADOOP_USER_NAME","root");
		System.setProperty("HADOOP_USER_PASSWORD","admin");
		String uri = "hdfs://hostMaster:9000/";
		Configuration config = new Configuration();
		fs = FileSystem.get(URI.create(uri),config);
	}
	@After
	public void end() throws Exception{
		fs.close();
	}

	@Test
	public void testListHdfs() throws Exception{
		orderFiles("/","\t");
	}
	public void orderFiles(String pathString,String prefix) throws Exception{
		FileStatus[] fileStatuses = fs.listStatus(new Path(pathString));
		for (FileStatus status:fileStatuses){
			System.out.println(prefix+status.getPath());
			if(status.isDirectory()){
				orderFiles(status.getPath().toString(),prefix+"\t");
			}
		}
	}

	@Test
	public void testCreateFile() throws Exception{
		FSDataOutputStream fsdos = fs.create(new Path("/txt/seq"));
		fsdos.write("Hello little there".getBytes());
		fsdos.flush();
		fsdos.close();
	}

	@Test
	public void testPrint() throws Exception {
		InputStream is = fs.open(new Path("/txt/资治通鉴.txt"));
//		IOUtils.copy(is,System.out);
		IOUtils.copyBytes(is, System.out, 1024, true);
	}
	@Test
	public void testSequence() throws Exception{
		Configuration config = new Configuration();
		Path path = new Path("hdfs://hostMaster:9000/txt/seq1");
		SequenceFile.Writer seqWriter = SequenceFile.createWriter(fs,config,path, Text.class,BytesWritable.class);
		File baseFile = new File("E:\\mnt\\file\\name1");
		for(File file:baseFile.listFiles()){
			seqWriter.append(new Text(file.getName()), new BytesWritable(FileUtils.readFileToByteArray(file)));
		}
		seqWriter.close();
	}
	@Test
	public void testLoadSequence() throws Exception{
		Configuration config = new Configuration();
		Path path = new Path("hdfs://hostMaster:9000/txt/seq1");
		SequenceFile.Reader seqReader = new SequenceFile.Reader(fs,path,config);
		Text key = new Text();
		BytesWritable value=new BytesWritable();
		while(seqReader.next(key,value)){
			System.out.print(key+":");
			value.getBytes();
			FileUtils.writeByteArrayToFile(new File("E:\\mnt\\file\\name2_load\\"+key),value.getBytes());
		}
	}
}
