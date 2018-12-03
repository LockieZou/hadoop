package com.sunvalley.hadoop.controller;

import com.sunvalley.hadoop.VO.BaseReturnVO;
import com.sunvalley.hadoop.util.HadoopUtil;
import org.apache.commons.lang.StringUtils;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

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
        // 创建空文件夹
        boolean isOk = HadoopUtil.mkdir(path);
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
        List<Map<String, Object>> list = HadoopUtil.readPathInfo(path);
        return new BaseReturnVO(list);
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
        HadoopUtil.createFile(path, file);
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
        String targetPath = HadoopUtil.readFile(path);
        return new BaseReturnVO(targetPath);
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
        List<Map<String, String>> returnList = HadoopUtil.listFile(path);
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
        boolean isOk = HadoopUtil.renameFile(oldName, newName);
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
        boolean isOk = HadoopUtil.deleteFile(path);
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
        HadoopUtil.uploadFile(path, uploadPath);
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
        HadoopUtil.downloadFile(path, downloadPath);
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
        HadoopUtil.copyFile(sourcePath, targetPath);
        return new BaseReturnVO("copy file success");
    }
}


