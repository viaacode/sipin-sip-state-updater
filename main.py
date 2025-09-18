from app.app import UpdaterService

if __name__ == "__main__":
    svc = UpdaterService()
    try:
        svc.start()
    finally:
        svc.stop()
