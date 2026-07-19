package net.lab1024.sa.base.module.support.file.service;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import lombok.extern.slf4j.Slf4j;
import net.lab1024.sa.base.common.code.SystemErrorCode;
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
 * SFTP remote file storage implementation.
 */
@Slf4j
public class FileStorageSftpServiceImpl implements IFileStorageService {

    private static final int CONNECT_TIMEOUT = 10000;

    private static final int SESSION_TIMEOUT = 30000;

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

    @Override
    public ResponseDTO<FileUploadVO> upload(MultipartFile file, String path) {
        if (file == null || file.isEmpty()) {
            return ResponseDTO.userErrorParam("上传文件不能为空");
        }

        Session session = null;
        ChannelSftp channelSftp = null;

        try {
            session = createSession();
            channelSftp = (ChannelSftp) session.openChannel("sftp");
            channelSftp.connect();

            String originalFilename = file.getOriginalFilename();
            String fileExtension = FilenameUtils.getExtension(originalFilename);
            String fileName = generateFileName();
            if (StringUtils.isNotBlank(fileExtension)) {
                fileName = fileName + "." + fileExtension;
            }

            String dateFolder = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy/MM/dd"));
            String fullPath = StringUtils.isBlank(path) ? dateFolder : path + "/" + dateFolder;
            fullPath = normalizePath(fullPath);

            String remoteFilePath = normalizePath(remotePath + "/" + fullPath);
            String remoteFileName = normalizePath(remoteFilePath + "/" + fileName);

            createRemoteDirectories(channelSftp, remoteFilePath);
            try (InputStream inputStream = file.getInputStream()) {
                channelSftp.put(inputStream, remoteFileName);
            }

            String fileKey = normalizePath(fullPath + "/" + fileName);
            FileUploadVO fileUploadVO = new FileUploadVO();
            fileUploadVO.setFileKey(fileKey);
            fileUploadVO.setFileName(originalFilename);
            fileUploadVO.setFileSize(file.getSize());
            fileUploadVO.setFileType(fileExtension);
            fileUploadVO.setFileUrl(joinUrl(urlPrefix, fileKey));
            return ResponseDTO.ok(fileUploadVO);
        } catch (Exception e) {
            log.error("SFTP file upload failed", e);
            return ResponseDTO.error(SystemErrorCode.SYSTEM_ERROR, "文件上传失败: " + e.getMessage());
        } finally {
            closeConnection(channelSftp, session);
        }
    }

    @Override
    public ResponseDTO<String> getFileUrl(String fileKey) {
        if (StringUtils.isBlank(fileKey)) {
            return ResponseDTO.userErrorParam("文件key不能为空");
        }
        return ResponseDTO.ok(joinUrl(urlPrefix, fileKey));
    }

    @Override
    public ResponseDTO<FileDownloadVO> download(String key) {
        if (StringUtils.isBlank(key)) {
            return ResponseDTO.userErrorParam("文件key不能为空");
        }

        Session session = null;
        ChannelSftp channelSftp = null;

        try {
            session = createSession();
            channelSftp = (ChannelSftp) session.openChannel("sftp");
            channelSftp.connect();

            String remoteFileName = normalizePath(remotePath + "/" + key);
            try (InputStream inputStream = channelSftp.get(remoteFileName);
                 ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    outputStream.write(buffer, 0, bytesRead);
                }

                FileMetadataVO metadata = new FileMetadataVO();
                metadata.setFileName(getFileNameFromKey(key));
                metadata.setFileFormat(getContentType(FilenameUtils.getExtension(key)));
                metadata.setFileSize((long) outputStream.size());

                FileDownloadVO downloadVO = new FileDownloadVO();
                downloadVO.setData(outputStream.toByteArray());
                downloadVO.setMetadata(metadata);
                return ResponseDTO.ok(downloadVO);
            }
        } catch (Exception e) {
            log.error("SFTP file download failed: {}", key, e);
            return ResponseDTO.error(SystemErrorCode.SYSTEM_ERROR, "文件下载失败: " + e.getMessage());
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
            session = createSession();
            channelSftp = (ChannelSftp) session.openChannel("sftp");
            channelSftp.connect();
            channelSftp.rm(normalizePath(remotePath + "/" + fileKey));
            return ResponseDTO.ok();
        } catch (Exception e) {
            log.error("SFTP file delete failed: {}", fileKey, e);
            return ResponseDTO.error(SystemErrorCode.SYSTEM_ERROR, "文件删除失败: " + e.getMessage());
        } finally {
            closeConnection(channelSftp, session);
        }
    }

    private Session createSession() throws JSchException {
        JSch jsch = new JSch();
        Session session = jsch.getSession(username, host, port);
        session.setPassword(password);
        session.setConfig("StrictHostKeyChecking", "no");
        session.setTimeout(SESSION_TIMEOUT);
        session.connect(CONNECT_TIMEOUT);
        return session;
    }

    private void createRemoteDirectories(ChannelSftp channelSftp, String targetPath) throws SftpException {
        try {
            channelSftp.cd(targetPath);
            return;
        } catch (SftpException ignored) {
            // Create below.
        }

        String currentPath = "";
        for (String dir : targetPath.split("/")) {
            if (StringUtils.isBlank(dir)) {
                continue;
            }

            currentPath += "/" + dir;
            try {
                channelSftp.cd(currentPath);
            } catch (SftpException e) {
                channelSftp.mkdir(currentPath);
                channelSftp.cd(currentPath);
            }
        }
    }

    private String generateFileName() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    private String getFileNameFromKey(String key) {
        int lastSlashIndex = key.lastIndexOf("/");
        return lastSlashIndex >= 0 ? key.substring(lastSlashIndex + 1) : key;
    }

    private String normalizePath(String path) {
        return path.replaceAll("/+", "/");
    }

    private String joinUrl(String prefix, String fileKey) {
        String cleanPrefix = prefix.endsWith("/") ? prefix.substring(0, prefix.length() - 1) : prefix;
        String cleanKey = fileKey.startsWith("/") ? fileKey.substring(1) : fileKey;
        return cleanPrefix + "/" + cleanKey;
    }

    private void closeConnection(ChannelSftp channelSftp, Session session) {
        if (channelSftp != null && channelSftp.isConnected()) {
            channelSftp.disconnect();
        }
        if (session != null && session.isConnected()) {
            session.disconnect();
        }
    }
}
