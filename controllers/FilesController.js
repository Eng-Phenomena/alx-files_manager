import { v4 as generateUUID } from 'uuid';
import { promises as fileSystem } from 'fs';
import { ObjectID as MongoObjectID } from 'mongodb';
import mimeTypes from 'mime-types';
import QueueSystem from 'bull';
import databaseClient from '../utils/db';
import cacheClient from '../utils/redis';

const uploadQueue = new QueueSystem('uploadQueue', 'redis://127.0.0.1:6379');

class StorageController {
  static async findUser(req) {
    const authToken = req.header('X-Token');
    const authKey = `auth_${authToken}`;
    const storedUserId = await cacheClient.get(authKey);
    if (storedUserId) {
      const userCollection = databaseClient.db.collection('users');
      const userObjectId = new MongoObjectID(storedUserId);
      const foundUser = await userCollection.findOne({ _id: userObjectId });
      if (!foundUser) {
        return null;
      }
      return foundUser;
    }
    return null;
  }

  static async handleUpload(req, res) {
    const currentUser = await StorageController.findUser(req);
    if (!currentUser) {
      return res.status(401).json({ error: 'Unauthorized' });
    }
    const { name: fileName } = req.body;
    const { type: fileType } = req.body;
    const { parentId: parentFileId } = req.body;
    const isPublicFile = req.body.isPublic || false;
    const { data: fileData } = req.body;
    if (!fileName) {
      return res.status(400).json({ error: 'Missing name' });
    }
    if (!fileType) {
      return res.status(400).json({ error: 'Missing type' });
    }
    if (fileType !== 'folder' && !fileData) {
      return res.status(400).json({ error: 'Missing data' });
    }

    const fileCollection = databaseClient.db.collection('files');
    if (parentFileId) {
      const parentObjectId = new MongoObjectID(parentFileId);
      const parentFile = await fileCollection.findOne({ _id: parentObjectId, userId: currentUser._id });
      if (!parentFile) {
        return res.status(400).json({ error: 'Parent not found' });
      }
      if (parentFile.type !== 'folder') {
        return res.status(400).json({ error: 'Parent is not a folder' });
      }
    }
    if (fileType === 'folder') {
      fileCollection.insertOne(
        {
          userId: currentUser._id,
          name: fileName,
          type: fileType,
          parentId: parentFileId || 0,
          isPublic: isPublicFile,
        },
      ).then((result) => res.status(201).json({
        id: result.insertedId,
        userId: currentUser._id,
        name: fileName,
        type: fileType,
        isPublic: isPublicFile,
        parentId: parentFileId || 0,
      })).catch((err) => {
        console.log(err);
      });
    } else {
      const storagePath = process.env.FOLDER_PATH || '/tmp/files_manager';
      const newFileName = `${storagePath}/${generateUUID()}`;
      const fileBuffer = Buffer.from(fileData, 'base64');
      try {
        try {
          await fileSystem.mkdir(storagePath);
        } catch (err) {
          // Ignore the error if the directory already exists
        }
        await fileSystem.writeFile(newFileName, fileBuffer, 'utf-8');
      } catch (err) {
        console.log(err);
      }
      fileCollection.insertOne(
        {
          userId: currentUser._id,
          name: fileName,
          type: fileType,
          isPublic: isPublicFile,
          parentId: parentFileId || 0,
          localPath: newFileName,
        },
      ).then((result) => {
        res.status(201).json(
          {
            id: result.insertedId,
            userId: currentUser._id,
            name: fileName,
            type: fileType,
            isPublic: isPublicFile,
            parentId: parentFileId || 0,
          },
        );
        if (fileType === 'image') {
          uploadQueue.add(
            {
              userId: currentUser._id,
              fileId: result.insertedId,
            },
          );
        }
      }).catch((err) => console.log(err));
    }
    return null;
  }

  static async showFile(req, res) {
    const currentUser = await StorageController.findUser(req);
    if (!currentUser) {
      return res.status(401).json({ error: 'Unauthorized' });
    }
    const fileId = req.params.id;
    const fileCollection = databaseClient.db.collection('files');
    const fileObjectId = new MongoObjectID(fileId);
    const foundFile = await fileCollection.findOne({ _id: fileObjectId, userId: currentUser._id });
    if (!foundFile) {
      return res.status(404).json({ error: 'Not found' });
    }
    return res.status(200).json(foundFile);
  }

  static async listFiles(req, res) {
    const currentUser = await StorageController.findUser(req);
    if (!currentUser) {
      return res.status(401).json({ error: 'Unauthorized' });
    }
    const { parentId: parentFileId, page: pageNumber } = req.query;
    const pageIndex = pageNumber || 0;
    const fileCollection = databaseClient.db.collection('files');
    let searchQuery;
    if (!parentFileId) {
      searchQuery = { userId: currentUser._id };
    } else {
      searchQuery = { userId: currentUser._id, parentId: MongoObjectID(parentFileId) };
    }
    fileCollection.aggregate(
      [
        { $match: searchQuery },
        { $sort: { _id: -1 } },
        {
          $facet: {
            metadata: [{ $count: 'total' }, { $addFields: { page: parseInt(pageIndex, 10) } }],
            data: [{ $skip: 20 * parseInt(pageIndex, 10) }, { $limit: 20 }],
          },
        },
      ],
    ).toArray((err, result) => {
      if (result) {
        const finalFiles = result[0].data.map((fileItem) => {
          const tempFile = {
            ...fileItem,
            id: fileItem._id,
          };
          delete tempFile._id;
          delete tempFile.localPath;
          return tempFile;
        });
        return res.status(200).json(finalFiles);
      }
      console.log('Error occurred');
      return res.status(404).json({ error: 'Not found' });
    });
    return null;
  }

  static async publishFile(req, res) {
    const currentUser = await StorageController.findUser(req);
    if (!currentUser) {
      return res.status(401).json({ error: 'Unauthorized' });
    }
    const { id: fileId } = req.params;
    const fileCollection = databaseClient.db.collection('files');
    const fileObjectId = new MongoObjectID(fileId);
    const updateObject = { $set: { isPublic: true } };
    const updateOptions = { returnOriginal: false };
    fileCollection.findOneAndUpdate({ _id: fileObjectId, userId: currentUser._id }, updateObject, updateOptions, (err, file) => {
      if (!file.lastErrorObject.updatedExisting) {
        return res.status(404).json({ error: 'Not found' });
      }
      return res.status(200).json(file.value);
    });
    return null;
  }

  static async unpublishFile(req, res) {
    const currentUser = await StorageController.findUser(req);
    if (!currentUser) {
      return res.status(401).json({ error: 'Unauthorized' });
    }
    const { id: fileId } = req.params;
    const fileCollection = databaseClient.db.collection('files');
    const fileObjectId = new MongoObjectID(fileId);
    const updateObject = { $set: { isPublic: false } };
    const updateOptions = { returnOriginal: false };
    fileCollection.findOneAndUpdate({ _id: fileObjectId, userId: currentUser._id }, updateObject, updateOptions, (err, file) => {
      if (!file.lastErrorObject.updatedExisting) {
        return res.status(404).json({ error: 'Not found' });
      }
      return res.status(200).json(file.value);
    });
    return null;
  }

  static async downloadFile(req, res) {
    const { id: fileId } = req.params;
    const fileCollection = databaseClient.db.collection('files');
    const fileObjectId = new MongoObjectID(fileId);
    fileCollection.findOne({ _id: fileObjectId }, async (err, file) => {
      if (!file) {
        return res.status(404).json({ error: 'Not found' });
      }
      if (file.isPublic) {
        if (file.type === 'folder') {
          return res.status(400).json({ error: "A folder doesn't have content" });
        }
        try {
          let downloadPath = file.localPath;
          const sizeParam = req.param('size');
          if (sizeParam) {
            downloadPath = `${downloadPath}_${sizeParam}`;
          }
          const fileData = await fileSystem.readFile(downloadPath);
          const fileMimeType = mimeTypes.contentType(file.name);
          return res.header('Content-Type', fileMimeType).status(200).send(fileData);
        } catch (err) {
          console.log(err);
          return res.status(404).json({ error: 'Not found' });
        }
      } else {
        const currentUser = await StorageController.findUser(req);
        if (!currentUser) {
          return res.status(404).json({ error: 'Not found' });
        }
        if (file.userId.toString() === currentUser._id.toString()) {
          if (file.type === 'folder') {
            return res.status(400).json({ error: "A folder doesn't have content" });
          }
          try {
            let downloadPath = file.localPath;
            const sizeParam = req.param('size');
            if (sizeParam) {
              downloadPath = `${downloadPath}_${sizeParam}`;
            }
            const fileMimeType = mimeTypes.contentType(file.name);
            return res.header('Content-Type', fileMimeType).status(200).sendFile(downloadPath);
          } catch (err) {
            console.log(err);
            return res.status(404).json({ error: 'Not found' });
          }
        } else {
          console.log(`Unauthorized access: file.userId=${file.userId}; currentUserId=${currentUser._id}`);
          return res.status(404).json({ error: 'Not found' });
        }
      }
    });
  }
}

module.exports = StorageController;

