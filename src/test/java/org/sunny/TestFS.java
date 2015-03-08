package org.sunny;

import static org.junit.Assert.*;

import java.io.FileNotFoundException;
import java.io.FilePermission;
import java.io.IOException;
import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFS {
	static Configuration conf;
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		conf = new Configuration();
		String pathConf = "hdfs://localhost/";
		System.out.println(conf.get("fs.defaultFS"));
		conf.set("fs.defaultFS",pathConf);
		System.out.println(conf.get("fs.defaultFS"));
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		FileContext.getFileContext().mkdir(new Path("./temp"),FsPermission.getDefault(),false);
	}

	@After
	public void tearDown() throws Exception {
		FileContext.getFileContext().delete(new Path("./temp"), true);
	}

	@Test
	public void createFileTest() throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, ParentNotDirectoryException, UnsupportedFileSystemException, IllegalArgumentException, IOException {
		String fileName = "./temp/test.txt";
		FSDataOutputStream outputStream = FileContext.getFileContext().create(new Path(fileName),EnumSet.of(CreateFlag.CREATE));
		FileStatus fileStatus = FileContext.getFileContext().getFileStatus(new Path(fileName));		 
		assertTrue(fileStatus.isFile()); 
	}
	
	@Test
	public void fileWriteWithoutFlush() throws IOException {
		String fileName = "./temp/test.txt";
		FSDataOutputStream outputStream = FileContext.getFileContext().create(new Path(fileName),EnumSet.of(CreateFlag.CREATE));
		outputStream.writeChars("Hello");
		//outputStream.close();
		FileStatus fileStatus = FileContext.getFileContext().getFileStatus(new Path(fileName));
		assertTrue(fileStatus.getLen() == 0);
	}
	@Test
	public void fileWriteWithFlushLengthCheck() throws IOException {
		String fileName = "./temp/test.txt";
		FSDataOutputStream outputStream = FileContext.getFileContext().create(new Path(fileName),EnumSet.of(CreateFlag.CREATE));
		outputStream.writeChars("Hello");
		outputStream.close();
		FileStatus fileStatus = FileContext.getFileContext().getFileStatus(new Path(fileName));
		System.out.println(fileStatus.getLen());
		assertTrue(fileStatus.getLen() == 10);
	}
	@Test
	public void fileWriteWithFlushReadCheck() throws IOException {
		String fileName = "./temp/test.txt";
		String data = "Hello";
		FSDataOutputStream outputStream = FileContext.getFileContext().create(new Path(fileName),EnumSet.of(CreateFlag.CREATE));
		outputStream.writeUTF(data);
		outputStream.close();
		FSDataInputStream inputStream = FileContext.getFileContext().open(new Path(fileName));
		String dataRead = inputStream.readUTF();
		System.out.println(dataRead);
		assertEquals(data,dataRead);
	}
}
