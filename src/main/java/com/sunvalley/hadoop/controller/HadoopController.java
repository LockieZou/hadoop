package com.sunvalley.hadoop.controller;

import com.sunvalley.hadoop.VO.BaseReturnVO;
import com.sunvalley.hadoop.util.HadoopUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 类或方法的功能描述 :TODO
 * @date: 2018-11-28 13:51
 */
@RestController
@RequestMapping("/hadoop")
public class HadoopController {

    /**
     * 创建文件夹
     *
     * @param path
     * @return
     * @throws Exception
     */
    @PostMapping("/mkdir")
    public BaseReturnVO mkdir(@RequestParam("path") String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            return new BaseReturnVO("请求参数为空");
        }
        // 文件对象
        FileSystem fs = HadoopUtil.getFileSystem();
        // 目标路径
        Path newPath = new Path(path);
        // 创建空文件夹
        boolean isOk = fs.mkdirs(newPath);
        fs.close();
        if (isOk) {
            return new BaseReturnVO("create dir success");
        } else {
            return new BaseReturnVO("create dir fail");
        }
    }

    /**
     * 读取HDFS目录信息
     *
     * @param path
     * @return
     * @throws Exception
     */
    @PostMapping("/readPathInfo")
    public BaseReturnVO readPathInfo(@RequestParam("path") String path) throws Exception {
        FileSystem fs = HadoopUtil.getFileSystem();
        Path newPath = new Path(path);
        FileStatus[] statusList = fs.listStatus(newPath);
        List<Map<String, Object>> list = new ArrayList<>();
        if (null != statusList && statusList.length > 0) {
            for (FileStatus fileStatus : statusList) {
                Map<String, Object> map = new HashMap<>();
                map.put("filePath", fileStatus.getPath());
                map.put("fileStatus", fileStatus.toString());
                list.add(map);
            }
            return new BaseReturnVO(list);
        } else {
            return new BaseReturnVO("目录内容为空");
        }
    }

    /**
     * 创建文件
     *
     * @param path
     * @return
     * @throws Exception
     */
    @PostMapping("/createFile")
    public BaseReturnVO createFile(@RequestParam("path") String path, @RequestParam("file") MultipartFile file) throws Exception {
        if (StringUtils.isEmpty(path) || null == file.getBytes()) {
            return new BaseReturnVO("请求参数为空");
        }
        String fileName = file.getOriginalFilename();
        FileSystem fs = HadoopUtil.getFileSystem();
        // 上传时默认当前目录，后面自动拼接文件的目录
        Path newPath = new Path(path + "/" + fileName);
        // 打开一个输出流
        FSDataOutputStream outputStream = fs.create(newPath);
        outputStream.write(file.getBytes());
        outputStream.close();
        fs.close();
        return new BaseReturnVO("create file success");
    }

    /**
     * 读取HDFS文件内容
     *
     * @param path
     * @return
     * @throws Exception
     */
    @PostMapping("/readFile")
    public BaseReturnVO readFile(@RequestParam("path") String path) throws Exception {
        FileSystem fs = HadoopUtil.getFileSystem();
        Path newPath = new Path(path);
        FSDataInputStream inputStream = null;
        try {
            inputStream = fs.open(newPath);
            // 防止中文乱码
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            String lineTxt = "";
            StringBuffer sb = new StringBuffer();
            while ((lineTxt = reader.readLine()) != null) {
                System.out.println(lineTxt);
                sb.append(lineTxt);
            }
            return new BaseReturnVO(sb);
        } finally {
            inputStream.close();
            fs.close();
        }
    }

    /**
     * 读取文件列表
     *
     * @param path
     * @return
     * @throws Exception
     */
    @PostMapping("/listFile")
    public BaseReturnVO listFile(@RequestParam("path") String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            return new BaseReturnVO("请求参数为空");
        }
        FileSystem fs = HadoopUtil.getFileSystem();
        Path newPath = new Path(path);
        // 递归找到所有文件
        RemoteIterator<LocatedFileStatus> filesList = fs.listFiles(newPath, true);
        List<Map<String, String>> returnList = new ArrayList<>();
        while (filesList.hasNext()) {
            LocatedFileStatus next = filesList.next();
            String fileName = next.getPath().getName();
            Path filePath = next.getPath();
            Map<String, String> map = new HashMap<>();
            map.put("fileName", fileName);
            map.put("filePath", filePath.toString());
            returnList.add(map);
        }
        fs.close();
        return new BaseReturnVO(returnList);
    }

    /**
     * 重命名文件
     *
     * @param oldName
     * @param newName
     * @return
     * @throws Exception
     */
    @PostMapping("/renameFile")
    public BaseReturnVO renameFile(@RequestParam("oldName") String oldName, @RequestParam("newName") String newName) throws Exception {
        if (StringUtils.isEmpty(oldName) || StringUtils.isEmpty(newName)) {
            return new BaseReturnVO("请求参数为空");
        }
        FileSystem fs = HadoopUtil.getFileSystem();
        Path oldPath = new Path(oldName);
        Path newPath = new Path(newName);
        boolean isOk = fs.rename(oldPath, newPath);
        fs.close();
        if (isOk) {
            return new BaseReturnVO("rename file success");
        } else {
            return new BaseReturnVO("rename file fail");
        }
    }

    /**
     * 删除文件
     *
     * @param path
     * @return
     * @throws Exception
     */
    @PostMapping("/deleteFile")
    public BaseReturnVO deleteFile(@RequestParam("path") String path) throws Exception {
        FileSystem fs = HadoopUtil.getFileSystem();
        Path newPath = new Path(path);
        boolean isOk = fs.deleteOnExit(newPath);
        fs.close();
        if (isOk) {
            return new BaseReturnVO("delete file success");
        } else {
            return new BaseReturnVO("delete file fail");
        }
    }

    /**
     * 上传文件
     *
     * @param path
     * @param uploadPath
     * @return
     * @throws Exception
     */
    @PostMapping("/uploadFile")
    public BaseReturnVO uploadFile(@RequestParam("path") String path, @RequestParam("uploadPath") String uploadPath) throws Exception {
        FileSystem fs = HadoopUtil.getFileSystem();
        // 上传路径
        Path clientPath = new Path(path);
        // 目标路径
        Path serverPath = new Path(uploadPath);

        // 调用文件系统的文件复制方法，第一个参数是否删除原文件true为删除，默认为false
        fs.copyFromLocalFile(false, clientPath, serverPath);
        fs.close();
        return new BaseReturnVO("upload file success");
    }

    /**
     * 下载文件
     * @param path
     * @param downloadPath
     * @return
     * @throws Exception
     */
    @PostMapping("/downloadFile")
    public BaseReturnVO downloadFile(@RequestParam("path") String path, @RequestParam("downloadPath") String downloadPath) throws Exception {
        FileSystem fs = HadoopUtil.getFileSystem();
        // 上传路径
        Path clientPath = new Path(path);
        // 目标路径
        Path serverPath = new Path(downloadPath);

        // 调用文件系统的文件复制方法，第一个参数是否删除原文件true为删除，默认为false
        fs.copyToLocalFile(false, clientPath, serverPath);
        fs.close();
        return new BaseReturnVO("download file success");
    }

    /**
     * HDFS文件复制
     * @param sourcePath
     * @param targetPath
     * @return
     * @throws Exception
     */
    @PostMapping("/copyFile")
    public BaseReturnVO copyFile(@RequestParam("sourcePath") String sourcePath, @RequestParam("targetPath") String targetPath) throws Exception {
        FileSystem fs = HadoopUtil.getFileSystem();
        // 原始文件路径
        Path oldPath = new Path(sourcePath);
        // 目标路径
        Path newPath = new Path(targetPath);

        FSDataInputStream inputStream = null;
        FSDataOutputStream outputStream = null;
        try {
            inputStream = fs.open(oldPath);
            outputStream = fs.create(newPath);

            IOUtils.copyBytes(inputStream, outputStream, 1024*1024*64,false);
            return new BaseReturnVO("copy file success");
        } finally {
            inputStream.close();
            outputStream.close();
            fs.close();
        }
    }
}


