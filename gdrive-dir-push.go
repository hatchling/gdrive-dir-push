package main

import (
	"flag"
	"fmt"
	"log"
	"mime"
	"os"
	"path/filepath"
	"time"

	humanize "github.com/dustin/go-humanize"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	drive "google.golang.org/api/drive/v2"

	"github.com/hatchling/gdrive-dir-push/directory_tree"
	"github.com/hatchling/gdrive-dir-push/oauth"
)

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	log.SetOutput(os.Stdout)
}

const (
	defaultClientId string = "902751591868-ghc6jn2vquj6s8n5v5np2i66h3dh5pqq.apps.googleusercontent.com"
	defaultSecret   string = "LLsUuv2NoLglNKx14t5dA9SC"
)

var (
	gDriveRootID   = flag.String("gdrive_root_id", "", "The ID of the Gdrive root folder to push to")
	localDirToPush = flag.String("local_dir_to_push", "", "Path to the local dir to push")
	oldFilesDir    = flag.String("old_files_dir", "", "The directory to move files that would otherwise be overwritten")
	maxOps         = flag.Int("max_gdrive_ops", 20, "Paranoia failsafe: the max number of Gdrive write ops this program will execute per run")
	verbose        = flag.Bool("verbose", false, "Whether to log verbosely to stdout")

	clientID     = flag.String("client_id", defaultClientId, "OAuth Client ID")
	clientSecret = flag.String("secret", defaultSecret, "OAuth Client Secret")
)

const folderMimeType = "application/vnd.google-apps.folder"

var opsExecuted int

// tallyOp keeps track of how many Gdrive write ops have been executed so far and kills the process
// if that number goes over the --max_gdrive_ops threshold.
func tallyOp() {
	opsExecuted++
	if opsExecuted > *maxOps {
		log.Fatalf("Oops, --max_gdrive_ops reached (%d) exiting", opsExecuted)
	}
}

type pusher struct {
	drv *drive.Service
}

// listFolder returns all files and folders directly under the GDrive parent folder |parentID|.  An
// error is returned if the operation fails.
func (p *pusher) listFolder(parentID string) ([]*drive.File, error) {
	if *verbose {
		fmt.Printf("listFolder(%s)\n", parentID)
	}
	query := fmt.Sprintf("'%s' in parents and trashed=false", parentID)
	call := p.drv.Files.List().Q(query)
	files := []*drive.File{}
	pageToken := ""
	for {
		if pageToken != "" {
			call.PageToken(pageToken)
		}
		r, err := call.Do()
		if err != nil {
			return nil, fmt.Errorf("Unable to list files: %v", err)
		}
		files = append(files, r.Items...)
		pageToken = r.NextPageToken
		if pageToken == "" {
			break
		}
	}
	return files, nil
}

// createFolder creates a new GDrive folder with |title| under the GDrive parent folder
// |parentID|.  It returns the ID of the created folder or an error if the operation fails.
func (p *pusher) createFolder(title, parentID string) (string, error) {
	tallyOp()
	if *verbose {
		fmt.Printf("createFolder(%s, %s)\n", title, parentID)
	}
	newFolder := &drive.File{
		Title:    title,
		MimeType: folderMimeType,
		Parents: []*drive.ParentReference{
			&drive.ParentReference{Id: parentID},
		},
	}
	r, err := p.drv.Files.Insert(newFolder).Do()
	if err != nil {
		return "", fmt.Errorf("Problem creating new GDrive folder: %v", err)
	}
	return r.Id, nil
}

// relocateFile moves |fileID| from the |oldParentID| folder to the --old_files_dir folder.  It
// returns an error if the operation fails.
func (p *pusher) relocateFile(fileID, oldParentID string) error {
	tallyOp()
	if *verbose {
		fmt.Printf("relocateFile(%s, %s)\n", fileID, oldParentID)
	}
	parentRef := &drive.ParentReference{Id: *oldFilesDir}
	if _, err := p.drv.Parents.Insert(fileID, parentRef).Do(); err != nil {
		return fmt.Errorf("An Insert() error occurred: %v", err)
	}
	if err := p.drv.Parents.Delete(fileID, oldParentID).Do(); err != nil {
		return fmt.Errorf("A Delete() error occurred: %v", err)
	}
	return nil
}

// createFile uploads |localfile| to the GDrive folder |parentID|.  It retries until |ctx| is
// cancelled.  It returns the ID of the created file or an error if the operation fails.
func (p *pusher) createFile(ctx context.Context, localFile *directory_tree.Node, parentID string) (string, error) {
	tallyOp()
	if *verbose {
		fmt.Printf("createFile(%v, %s)", localFile, parentID)
	}
	file, err := os.Open(localFile.FullPath)
	if err != nil {
		log.Fatal(err)
	}
	title := filepath.Base(localFile.FullPath)
	mimeType := mime.TypeByExtension(filepath.Ext(title))

	// File instance
	f := &drive.File{
		Title:    title,
		MimeType: mimeType,
		Parents: []*drive.ParentReference{
			&drive.ParentReference{Id: parentID},
		},
	}

	// TODO print info about the transfer
	r, err := p.drv.Files.Insert(f).ResumableMedia(ctx, file, localFile.Info.Size, mimeType).Do()
	if err != nil {
		return "", fmt.Errorf("An error occurred uploading the file: %v\n", err)
	}
	return r.Id, nil
}

// processNode recursively makes write operations to sync the local file structure described by
// |node| with GDrive.  It will retry until |ctx| is cancelled. It returns an error is any operation
// fails.
func (p *pusher) processNode(ctx context.Context, node *directory_tree.Node) error {
	if *verbose {
		fmt.Printf("processNode(ctx, %v)", node)
	}
	remoteItems, err := p.listFolder(node.DriveID)
	if err != nil {
		return fmt.Errorf("Problem listing GDrive folder: %v", err)
	}
	// TODO: Handle case where remote type != local type
	for _, localItem := range node.Children {
		var found bool
		relName, err := filepath.Rel(*localDirToPush, localItem.FullPath)
		if err != nil {
			log.Fatalf("Could not determine relative path: %v", err)
		}
		for _, remoteItem := range remoteItems {
			if remoteItem.Title == localItem.Info.Name {
				localItem.DriveID = remoteItem.Id
				found = true
				break
			}
		}
		statusPrefix := " "
		if localItem.Info.IsDir {
			// Handle folders
			if !found {
				statusPrefix = "+"
				// No GDrive folder exists, create it under the current parent
				newID, err := p.createFolder(localItem.Info.Name, node.DriveID)
				if err != nil {
					return fmt.Errorf("Problem creating GDrive folder %q: %v", relName, err)
				}
				localItem.DriveID = newID
			}
			fmt.Printf("%s /%s/\n", statusPrefix, relName)
		} else {
			// Handle files
			statusPrefix = "+"
			if found {
				statusPrefix = "M"
				if err := p.relocateFile(localItem.DriveID, node.DriveID); err != nil {
					return fmt.Errorf("Problem relocating GDrive file %q: %v", relName, err)
				}
			}
			newID, err := p.createFile(ctx, localItem, node.DriveID)
			if err != nil {
				return fmt.Errorf("Problem creating Gdrive file %q: %v", relName, err)
			}
			localItem.DriveID = newID
			fmt.Printf("%s /%s (%s)\n", statusPrefix, relName, humanize.Bytes(uint64(localItem.Info.Size)))
		}
		if localItem.Info.IsDir {
			// Recursively handle directories (but print status first)
			if err := p.processNode(ctx, localItem); err != nil {
				return err
			}
		}
	}
	return nil
}

// driveClient prepares a Drive client to use for GDrive operations.
func driveClient(ctx context.Context) (*drive.Service, error) {
	config := &oauth2.Config{
		ClientID:     *clientID,
		ClientSecret: *clientSecret,
		Endpoint:     google.Endpoint,
		RedirectURL:  "urn:ietf:wg:oauth:2.0:oob",
		Scopes:       []string{"https://www.googleapis.com/auth/drive"},
	}
	client := oauth.GetClient(ctx, config)

	drv, err := drive.New(client)
	if err != nil {
		return nil, fmt.Errorf("Unable to retrieve drive Client %v", err)
	}
	return drv, nil
}

func main() {
	flag.Parse()

	if *gDriveRootID == "" {
		log.Fatalf("--gdrive_root_id must be provided")
	}
	if *localDirToPush == "" {
		log.Fatalf("--local_dir_to_push must be provided")
	}
	if *oldFilesDir == "" {
		log.Fatalf("--old_files_dir must be provided")
	}

	absPath, err := filepath.Abs(*localDirToPush)
	if err != nil {
		log.Fatalf("Could not determine absolute path: %v", err)
	}
	*localDirToPush = absPath

	start := time.Now()
	fmt.Printf("Pushing contents of %q to GDrive folder %q\n\n", *localDirToPush, *gDriveRootID)
	fmt.Printf("%v\n", start)

	ctx := context.Background()

	drv, err := driveClient(ctx)
	if err != nil {
		log.Fatalf("Problem creating Drive client: %v", err)
	}

	pusher := pusher{
		drv: drv,
	}

	tree, err := directory_tree.NewTree(*localDirToPush)
	if err != nil {
		log.Fatalf("Problem creating directory_tree: %v", err)
	}

	// Fill in the root node with the provided ID
	tree.DriveID = *gDriveRootID
	if err := pusher.processNode(ctx, tree); err != nil {
		log.Fatalf("Problem syncing dir: %v", err)
	}

	fmt.Printf("Took %v\n", time.Since(start))
}
