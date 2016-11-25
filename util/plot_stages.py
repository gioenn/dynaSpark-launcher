import os

parent = "./results/OK/"
for folder in [parent + d for d in os.listdir(parent) if os.path.isdir(os.path.join(parent, d))]:
    for fold in [folder + "/" + d for d in os.listdir(folder) if
                 os.path.isdir(os.path.join(folder, d))]:
        if fold.split("/")[-1] != "Native":
            for app in [fold + "/" + d for d in os.listdir(fold) if
                        os.path.isdir(os.path.join(fold, d))]:
                print(app)
                if "sort" in app:
                    import plot
                    plot.plot(app)

# import plot
# plot.plot("./results/OK/SVM/40%/app-20161028191404-0000")
