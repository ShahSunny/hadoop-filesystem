package org.sunny;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class App {
	

public static void main(String[] args) throws Exception {
	int exitcode = ToolRunner.run(new AppRunner(), args);
	System.exit(exitcode);
}

public static class AppRunner extends Configured implements Tool {
	public AppRunner() {
		super();
	}
	
	public int run(String[] args)  throws Exception {
		Configuration conf = getConf();
		conf.set("fs.defaultFS", "file:///");
		conf.set("mapreduce.framework.name", "local");
		setConf(conf);
		String compressedFile = args[0];//"/home/sunny/ncdc_data/ftp.ncdc.noaa.gov/pub/data/noaa/1981/992210-99999-1981.gz";
		String compressedFileWithoutSuffix = getFileNameWithoutSuffix(compressedFile);
		String uncompressedFile = compressedFileWithoutSuffix + ".bk";
		String originallyUncompressedFile = compressedFileWithoutSuffix;
		uncompressFile(compressedFile,uncompressedFile);
		String processedFileChecksum 				= getChecksum(uncompressedFile);
		String originallyUncompressedFileChecksum 	= getChecksum(originallyUncompressedFile);
		if(processedFileChecksum.equals(originallyUncompressedFileChecksum)) {
			System.out.println("Files are same");
		} else {
			System.out.println("Files are not same");
			System.out.println("processedFileChecksum = " + processedFileChecksum);
			System.out.println("originallyUncompressedFileChecksum = " + originallyUncompressedFileChecksum);
		}
		return 0;
	}
	
	private String getFileNameWithoutSuffix(String compressedFile) {
		Path path = new Path(compressedFile);
		CompressionCodecFactory factory = new CompressionCodecFactory(getConf());
	    CompressionCodec codec = factory.getCodec(path);
		String outputUri = CompressionCodecFactory.removeSuffix(path.toString(), codec.getDefaultExtension());
		return outputUri;
	}

	private String getChecksum(String filePath) {
		String md5 = null;
		try{
			FileInputStream fis = new FileInputStream(new File(filePath));
			md5 = org.apache.commons.codec.digest.DigestUtils.md5Hex(fis);
			fis.close();
		} catch(IOException e) {
			e.printStackTrace();
		}
		return md5;
	}

	private void uncompressFile(String compressedFile, String uncompressedFile) throws IOException {
		OutputStream out = null;
		CompressionInputStream in = null;
		try{
			out = new BufferedOutputStream(new FileOutputStream(uncompressedFile));
			Path path = new Path(compressedFile);		
			CompressionCodecFactory factory = new CompressionCodecFactory(getConf());
		    CompressionCodec codec = factory.getCodec(path);
		    in = codec.createInputStream(new BufferedInputStream(new FileInputStream(compressedFile)));
		    IOUtils.copyBytes(in, out, 4096, false);
		} catch(IOException e) {
			e.printStackTrace();
		} catch(NullPointerException e) {
			e.printStackTrace();
		} finally {
			if(in != null) {
				in.close();
			}
			if(out != null) {
				out.close();
			}
		}
	}	
	    
}

}