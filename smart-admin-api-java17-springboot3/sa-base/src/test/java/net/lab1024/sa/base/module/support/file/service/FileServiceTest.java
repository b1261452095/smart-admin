package net.lab1024.sa.base.module.support.file.service;

import net.lab1024.sa.base.common.domain.ResponseDTO;
import net.lab1024.sa.base.module.support.file.constant.FileFolderTypeEnum;
import net.lab1024.sa.base.module.support.file.dao.FileDao;
import net.lab1024.sa.base.module.support.file.domain.entity.FileEntity;
import net.lab1024.sa.base.module.support.file.domain.vo.FileUploadVO;
import net.lab1024.sa.base.module.support.securityprotect.service.SecurityFileService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.mock.web.MockMultipartFile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FileServiceTest {

    @Mock
    private IFileStorageService fileStorageService;

    @Mock
    private FileDao fileDao;

    @Mock
    private SecurityFileService securityFileService;

    @InjectMocks
    private FileService fileService;

    @Test
    void shouldGenerateFileUrlAfterSavingFileRecord() {
        MockMultipartFile file = new MockMultipartFile(
                "file",
                "banner.jpg",
                "image/jpeg",
                new byte[]{1, 2, 3}
        );
        String fileKey = "private/common/banner.jpg";
        String signedUrl = "https://example.cos.test/" + fileKey + "?X-Amz-Signature=test";
        FileUploadVO uploadVO = new FileUploadVO();
        uploadVO.setFileKey(fileKey);
        uploadVO.setFileType("jpg");

        when(securityFileService.checkFile(file)).thenReturn(ResponseDTO.ok());
        when(fileStorageService.upload(file, FileFolderTypeEnum.COMMON.getFolder()))
                .thenReturn(ResponseDTO.ok(uploadVO));
        when(fileDao.insert(any(FileEntity.class))).thenAnswer(invocation -> {
            FileEntity entity = invocation.getArgument(0);
            entity.setFileId(1L);
            return 1;
        });
        when(fileStorageService.getFileUrl(fileKey)).thenReturn(ResponseDTO.ok(signedUrl));

        ResponseDTO<FileUploadVO> response = fileService.fileUpload(
                file,
                FileFolderTypeEnum.COMMON.getValue(),
                null
        );

        assertThat(response.getOk()).isTrue();
        assertThat(response.getData().getFileId()).isEqualTo(1L);
        assertThat(response.getData().getFileUrl()).isEqualTo(signedUrl);

        InOrder order = inOrder(fileStorageService, fileDao);
        order.verify(fileStorageService).upload(eq(file), eq(FileFolderTypeEnum.COMMON.getFolder()));
        order.verify(fileDao).insert(any(FileEntity.class));
        order.verify(fileStorageService).getFileUrl(fileKey);
    }
}
