package net.lab1024.sa.base.module.support.file.service;

import com.jcraft.jsch.*;
import lombok.extern.slf4j.Slf4j;
import net.lab1024.sa.base.common.code.UserErrorCode;
import net.lab1024.sa.base.common.domain.ResponseDTO;
import net.lab1024.sa.base.module.support.file.domain.vo.FileDownloadVO;
import net.lab1024.sa.base.module.support.file.domain.vo.FileMetadataVO;
import net.lab1024.sa.base.module.support.file.domain.vo.FileUploadVO;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.multipart.MultipartFile;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

/**
 * SFTP 远程服务器文件存储实现
 *
 * @Author 学习者
 * @Date 2024-11-26
 */
@Slf4j
public class FileStorageSftpServiceImpl implements IFileStorageService {

    @Value("${file.storage.sftp.host}")
    private String host;

    @Value("${file.storage.sftp.port}")
    private Integer port;

    @Value("${file.storage.sftp.username}")
    private String username;

    @Value("${file.storage.sftp.password}")
    private String password;

    @Value("${file.storage.sftp.remote-path}")
    private String remotePath;

    @Value("${file.storage.sftp.url-prefix}")
    private String urlPrefix;

    private static final int CONNECT_TIMEOUT = 10000; // 10秒连接超时
    private static final int SESSION_TIMEOUT = 30000; // 30秒会话超时

    @Override
    public ResponseDTO<FileUploadVO> upload(MultipartFile file, String path) {
        if (file == null || file.isEmpty()) {
            return ResponseDTO.userErrorParam("文件不能为空");
        }

        Session session = null;
        ChannelSftp channelSftp = null;

        try {
            // 1. 建立 SFTP 连接
            session = createSession();
            channelSftp = (ChannelSftp) session.openChannel("sftp");
            channelSftp.connect();

            // 2. 生成文件名和路径
            String originalFilename = file.getOriginalFilename();
            String fileExtension = getFileExtension(originalFilename);
            String fileName = generateFileName() + "." + fileExtension;
            
            // 3. 构建远程文件路径
            String dateFolder = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy/MM/dd"));
            String fullPath = StringUtils.isBlank(path) ? dateFolder : path + "/" + dateFolder;
            
            // 规范化路径，移除多余的斜杠
            fullPath = fullPath.replaceAll("/+", "/");
            String remoteFilePath = (remotePath + "/" + fullPath).replaceAll("/+", "/");
            String remoteFileName = (remoteFilePath + "/" + fileName).replaceAll("/+", "/");
            
            log.info("文件路径信息 - 原始路径: {}, 日期文件夹: {}, 完整路径: {}, 远程文件路径: {}, 远程文件名: {}", 
                     path, dateFolder, fullPath, remoteFilePath, remoteFileName);

            // 4. 创建远程目录
            createRemoteDirectories(channelSftp, remoteFilePath);

            // 5. 上传文件
            try (InputStream inputStream = file.getInputStream()) {
                channelSftp.put(inputStream, remoteFileName);
            }

            // 6. 构建返回结果
            FileUploadVO fileUploadVO = new FileUploadVO();
            fileUploadVO.setFileKey(fullPath + "/" + fileName);
            fileUploadVO.setFileName(originalFilename);
            fileUploadVO.setFileSize(file.getSize());
            fileUploadVO.setFileType(FilenameUtils.getExtension(originalFilename));
            // 生成访问URL
            String fileKey = (fullPath + "/" + fileName).replaceAll("/+", "/");
            fileUploadVO.setFileUrl(urlPrefix + fileKey);

            log.info("SFTP文件上传成功: {} -> {}", originalFilename, remoteFileName);
            log.info("生成的文件访问URL: {}", fileUploadVO.getFileUrl());
            return ResponseDTO.ok(fileUploadVO);

        } catch (Exception e) {
            log.error("SFTP文件上传失败", e);
            return ResponseDTO.error(UserErrorCode.DATA_NOT_EXIST, "文件上传失败: " + e.getMessage());
        } finally {
            // 7. 关闭连接
            closeConnection(channelSftp, session);
        }
    }

    @Override
    public ResponseDTO<String> getFileUrl(String fileKey) {
        if (StringUtils.isBlank(fileKey)) {
            return ResponseDTO.userErrorParam("文件key不能为空");
        }
        
        String url = urlPrefix + "/" + fileKey;
        return ResponseDTO.ok(url);
    }

    @Override
    public ResponseDTO<FileDownloadVO> download(String key) {
        if (StringUtils.isBlank(key)) {
            return ResponseDTO.userErrorParam("文件key不能为空");
        }

        Session session = null;
        ChannelSftp channelSftp = null;

        try {
            // 1. 建立 SFTP 连接
            session = createSession();
            channelSftp = (ChannelSftp) session.openChannel("sftp");
            channelSftp.connect();

            // 2. 构建远程文件路径
            String remoteFileName = remotePath + "/" + key;

            // 3. 下载文件
            try (InputStream inputStream = channelSftp.get(remoteFileName);
                 ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                
                byte[] buffer = new byte[1024];
                int bytesRead;
                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    outputStream.write(buffer, 0, bytesRead);
                }

                // 4. 构建返回结果
                FileDownloadVO downloadVO = new FileDownloadVO();
                downloadVO.setData(outputStream.toByteArray());
                
                // 设置文件元数据
                FileMetadataVO metadata = new FileMetadataVO();
                metadata.setFileName(getFileNameFromKey(key));
                metadata.setFileFormat(getContentType(getFileExtension(key)));
                metadata.setFileSize((long) outputStream.size());
                downloadVO.setMetadata(metadata);

                return ResponseDTO.ok(downloadVO);
            }

        } catch (Exception e) {
            log.error("SFTP文件下载失败: {}", key, e);
            return ResponseDTO.error(UserErrorCode.DATA_NOT_EXIST, "文件下载失败: " + e.getMessage());
        } finally {
            closeConnection(channelSftp, session);
        }
    }

    @Override
    public ResponseDTO<String> delete(String fileKey) {
        if (StringUtils.isBlank(fileKey)) {
            return ResponseDTO.userErrorParam("文件key不能为空");
        }

        Session session = null;
        ChannelSftp channelSftp = null;

        try {
            // 1. 建立 SFTP 连接
            session = createSession();
            channelSftp = (ChannelSftp) session.openChannel("sftp");
            channelSftp.connect();

            // 2. 删除远程文件
            String remoteFileName = remotePath + "/" + fileKey;
            channelSftp.rm(remoteFileName);

            log.info("SFTP文件删除成功: {}", remoteFileName);
            return ResponseDTO.ok("删除成功");

        } catch (Exception e) {
            log.error("SFTP文件删除失败: {}", fileKey, e);
            return ResponseDTO.error(UserErrorCode.DATA_NOT_EXIST, "文件删除失败: " + e.getMessage());
        } finally {
            closeConnection(channelSftp, session);
        }
    }

    /**
     * 创建 SFTP 会话
     */
    private Session createSession() throws JSchException {
        log.info("正在连接 SFTP 服务器: {}:{}, 用户: {}", host, port, username);
        
        JSch jsch = new JSch();
        Session session = jsch.getSession(username, host, port);
        session.setPassword(password);
        
        // 跳过主机密钥检查（生产环境建议配置已知主机）
        session.setConfig("StrictHostKeyChecking", "no");
        session.setTimeout(SESSION_TIMEOUT);
        
        try {
            session.connect(CONNECT_TIMEOUT);
            log.info("SFTP 连接成功");
            return session;
        } catch (JSchException e) {
            log.error("SFTP 连接失败 - 主机: {}, 端口: {}, 用户: {}, 错误: {}", 
                     host, port, username, e.getMessage());
            throw e;
        }
    }

    /**
     * 创建远程目录
     */
    private void createRemoteDirectories(ChannelSftp channelSftp, String remotePath) throws SftpException {
        try {
            // 先尝试切换到目标目录
            channelSftp.cd(remotePath);
            log.debug("目录已存在: {}", remotePath);
        } catch (SftpException e) {
            // 目录不存在，需要创建
            log.debug("目录不存在，开始创建: {}", remotePath);
            
            // 分解路径并逐级创建
            String[] dirs = remotePath.split("/");
            String currentPath = "";
            
            for (String dir : dirs) {
                if (StringUtils.isBlank(dir)) {
                    continue;
                }
                
                currentPath += "/" + dir;
                try {
                    channelSftp.cd(currentPath);
                    log.debug("切换到目录: {}", currentPath);
                } catch (SftpException ex) {
                    // 目录不存在，创建目录
                    try {
                        channelSftp.mkdir(currentPath);
                        channelSftp.cd(currentPath);
                        log.debug("创建并切换到目录: {}", currentPath);
                    } catch (SftpException mkdirEx) {
                        log.error("创建目录失败: {}, 错误: {}", currentPath, mkdirEx.getMessage());
                        throw mkdirEx;
                    }
                }
            }
        }
    }

    /**
     * 生成唯一文件名
     */
    private String generateFileName() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    /**
     * 获取文件扩展名
     */
    private String getFileExtension(String fileName) {
        if (StringUtils.isBlank(fileName)) {
            return "";
        }
        int lastDotIndex = fileName.lastIndexOf(".");
        return lastDotIndex > 0 ? fileName.substring(lastDotIndex + 1) : "";
    }

    /**
     * 从文件key中获取文件名
     */
    private String getFileNameFromKey(String key) {
        if (StringUtils.isBlank(key)) {
            return "download";
        }
        int lastSlashIndex = key.lastIndexOf("/");
        return lastSlashIndex > 0 ? key.substring(lastSlashIndex + 1) : key;
    }

    /**
     * 关闭连接
     */
    private void closeConnection(ChannelSftp channelSftp, Session session) {
        if (channelSftp != null && channelSftp.isConnected()) {
            channelSftp.disconnect();
        }
        if (session != null && session.isConnected()) {
            session.disconnect();
        }
    }
}
