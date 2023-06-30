import tensorflow as tf
import matplotlib.pyplot as plt
import argparse, pandas as pd, sklearn, numpy as np, os
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.linear_model import LogisticRegression
from imblearn.over_sampling import SMOTE, RandomOverSampler

parser = argparse.ArgumentParser(description='Trains a neural network on a training data')
parser.add_argument('--epochs', 
                    metavar='-e',
                    type=int,
                    help='Enter the training epoch')
parser.add_argument('--batchSize', 
                    metavar='-b', 
                    type=int,
                    help='Enter the batch size')

args = parser.parse_args()
epochs = args.epochs
batchSize = args.batchSize

def dataFile():
    dataset = pd.read_csv(os.getcwd()+'\\dataset.csv')
    dataset['City Or County'] = dataset['City Or County'].astype('category')
    dataset['City Or County'] = dataset['City Or County'].cat.codes
    dataset['State'] = dataset['State'].astype('category')
    dataset['State'] = dataset['State'].cat.codes
    labels = dataset[['State', 'City Or County']]
    dataset.drop(columns=['Incident Date','State','City Or County'], inplace=True)
    trainLen = int(len(dataset)*0.8)
    numClasses1 = len(labels['State'].unique())
    numClasses2 = len(labels['City Or County'].unique())
    sm = RandomOverSampler(random_state=123)
    dataset, labels = sm.fit_resample(dataset.to_numpy(), labels.to_numpy()[:,1])
    ind = np.arange(len(dataset))
    np.random.shuffle(ind)
    dataset = dataset[ind,:]
    labels = labels[ind]
    print(dataset.shape, labels.shape)
    xTrain, xTest, yTrain, yTest = train_test_split(dataset, labels,
                                                                    test_size=0.2, random_state=123)
    
    xTrain = xTrain.reshape((xTrain.shape[0], xTrain.shape[1], 1))
    xTest = xTest.reshape((xTest.shape[0], xTest.shape[1], 1))
    return xTrain, yTrain, xTest, yTest, numClasses2

#xTrain, yTrain, xTest, yTest, numClasses = dataFile()

def nnModel(inputShape, lenClasses):
    inputLayer = tf.keras.layers.Input(inputShape)

    conv1 = tf.keras.layers.Conv1D(filters=128, kernel_size=3,padding="same")(inputLayer)
    conv1 = tf.keras.layers.BatchNormalization()(conv1)
    conv1 = tf.keras.layers.ReLU()(conv1)

    conv2 = tf.keras.layers.Conv1D(filters=128, kernel_size=3, padding="same")(conv1)
    conv2 = tf.keras.layers.BatchNormalization()(conv2)
    conv2 = tf.keras.layers.ReLU()(conv2)

    conv3 = tf.keras.layers.Conv1D(filters=64, kernel_size=3, padding="same")(conv2)
    conv3 = tf.keras.layers.BatchNormalization()(conv2)
    conv3 = tf.keras.layers.ReLU()(conv3)

    gap = tf.keras.layers.GlobalAveragePooling1D()(conv3)
    
    outputLayer = tf.keras.layers.Dense(lenClasses, activation="softmax")(gap)

    return tf.keras.Model(inputs=inputLayer, outputs=outputLayer)
    
print(numClasses)
model = nnModel(inputShape=xTrain.shape[1:], lenClasses=numClasses)
#tf.keras.utils.plot_model(model, show_shapes=True)

batch_size = batchSize

callbacks = [
    tf.keras.callbacks.ModelCheckpoint(
        "best_model2.h5", save_best_only=True, monitor="val_loss"
    ),
    tf.keras.callbacks.ReduceLROnPlateau(
        monitor="val_loss", factor=0.5, patience=20, min_lr=0.0001
    ),
    tf.keras.callbacks.EarlyStopping(monitor="val_loss", patience=50, verbose=1),
]
model.compile(
    optimizer="adam",
    loss="sparse_categorical_crossentropy",
    metrics=["sparse_categorical_accuracy"],
)
history = model.fit(
    xTrain,
    [yTrain],
    batch_size=batch_size,
    epochs=epochs,
    callbacks=callbacks,
    validation_split=0.2,
    verbose=1,
)
